###############################################################################################
# DAG ID: dbx_dom_global_daily
# Description: Merged DAG combining OMS/OBF streaming 
#              and dynamic Databricks Scala JAR jobs.
#
# Author      : Pavan Kalyan Kotha
# Last Updated: May 02, 2026
###############################################################################################

import json
import logging
import time
from datetime import datetime, timedelta
from json import dumps, loads

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import DAG, Variable, DagRun, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.session import create_session
from airflow.utils.trigger_rule import TriggerRule
from dap.operators.platform_notify_operator import PlatformNotifyOperator


class ProductMasterSensor(BaseSensorOperator):
    """
    Gate sensor: returns True (= done) when na_product_master_build_gen2 is NOT
    in any active state.  Uses mode='reschedule' so the worker slot is
    released between pokes — no other tasks are starved.

    Queries live TaskInstance state directly, so it works for both scheduled
    and manually-triggered product_master runs regardless of execution_date.
    """

    ACTIVE_STATES = {"running", "queued", "up_for_retry", "scheduled", "up_for_reschedule"}

    def __init__(
        self,
        pm_dag_id="process_na_product_master_daily",
        pm_task_id="na_product_master_build_gen2",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.pm_dag_id = pm_dag_id
        self.pm_task_id = pm_task_id

    def poke(self, context):
        with create_session() as session:
            count = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == self.pm_dag_id,
                    TaskInstance.task_id == self.pm_task_id,
                    TaskInstance.state.in_(self.ACTIVE_STATES),
                )
                .count()
            )

        if count > 0:
            self.log.info(
                " '%s.%s' is still active (%d instance(s)). Will retry...",
                self.pm_dag_id, self.pm_task_id, count,
            )
            return False

        self.log.info(
            " '%s.%s' is not active. Gateway cleared — proceeding.",
            self.pm_dag_id, self.pm_task_id,
        )
        return True

##############################################################################################
# Variables & Configuration
##############################################################################################

dag_id = "dbx_dom_global_daily"
local_tz = pendulum.timezone("US/Eastern")

dom_json = Variable.get("dom_prod_json", deserialize_json=True)
schedule_interval = dom_json["schedule_interval"]
dbt_default_args = dom_json.get("dbt_default_args", {})

# ── OMS streaming job parameters (AdlsLoadMain_Gen2) ──────────────────────────────────────
dom_oms_scala = dom_json.get("dom_oms_scala", {})
oms_main_class = dom_oms_scala.get("mainclass")
oms_jar_path = dom_oms_scala.get("jar_path")
oms_spark_version = dom_oms_scala.get("spark_version")
oms_node_type_id = dom_oms_scala.get("node_type_id")
oms_instance_pool_id = dom_oms_scala.get("instance_pool_id")
oms_cluster_scale = dom_oms_scala.get("cluster_scale", {})
oms_params = dom_json.get("dom_oms_params", [])

# ── OBF streaming job parameters (AdlsObfStreamMain_Gen2) ─────────────────────────────────
dom_obf_scala = dom_json.get("dom_obf_scala", {})
obf_main_class = dom_obf_scala.get("mainclass")
obf_jar_path = dom_obf_scala.get("jar_path")
obf_spark_version = dom_obf_scala.get("spark_version")
obf_node_type_id = dom_obf_scala.get("node_type_id")
obf_instance_pool_id = dom_obf_scala.get("instance_pool_id")
obf_cluster_scale = dom_obf_scala.get("cluster_scale", {})
obf_params = dom_json.get("dom_obf_params", [])

run_oms_obf_streaming_job = dom_json.get("run_oms_obf_streaming_job", False)

# ── Databricks Scala JAR cluster config (UC jobs) ─────────────────────────────────────────
dom_scala = dom_json.get("dom_scala", {})
spark_version = dom_scala.get("spark_version")
node_type_id = dom_scala.get("node_type_id")
instance_pool_id = dom_scala.get("instance_pool_id")
main_class = dom_scala.get("mainclass")
jar_path = dom_scala.get("jar_path")
cluster_scale = dom_scala.get("cluster_scale", {})
data_security_mode = dom_scala.get("data_security_mode", "USER_ISOLATION")

# ── Databricks policy IDs ──────────────────────────────────────────────────────────────────
common_databricks_policies = Variable.get("common_databricks_policies", deserialize_json=True)
oms_policy_id = common_databricks_policies.get("Data-Engineer-Job-Pool-Policy-Non-UC")
obf_policy_id = common_databricks_policies.get("Data-Engineer-Job-Pool-Policy-Non-UC")
policy_id = common_databricks_policies.get("Data-Engineer-Job-Pool-Policy-Iceberg")

##############################################################################################
# DAG Definition
##############################################################################################

with DAG(
    dag_id=dag_id,
    description="Merged OMS/OBF streaming + dynamic Databricks Scala JAR jobs",
    default_args=dbt_default_args,
    schedule_interval=schedule_interval,
    start_date=datetime(2024, 1, 25, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["dom", "databricks", "oms", "obf", "streaming", "scala", "uc"],
) as dag:

    ##############################################################################################
    # Custom Tags
    ##############################################################################################
    custom_tags = {
        "SparkAppName": dag_id,
        "SparkAppRunDate": "{{ ds }}",
        "SparkClusterType": "job",
        "SparkApplicationType": "batch",
        "SparkApplicationSubType": "processing",
    }

    ##############################################################################################
    # Notify Details
    ##############################################################################################
    notify_template = loads(Variable.get("notify_json"))
    notify_template["application_name"] = dag_id
    notify_template["application_link"] = "{}{}".format(
        notify_template["application_link"], dag_id
    )
    notify_template["notification_types"] = ["teams", "servicenow"]
    notify_template["message"] = "Job Failed"
    notify_template["status"] = "FAILED"

    job_failure_notification_json = dumps(notify_template)
    job_failure_notification = PlatformNotifyOperator(
        task_id="job_failure_notification",
        notification=job_failure_notification_json,
        provide_context=True,
        dag=dag,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    ##############################################################################################
    # Helper Functions
    ##############################################################################################
    def failtask():
        raise ValueError("DAG Failure")

    def python_method(ds, var_key="dbt_dom_global_daily_ExecutionTime", **kwargs):
        """
        Store execution_date in an Airflow Variable.
        Each per-group task passes its own unique var_key via op_kwargs
        to avoid parallel INSERT race conditions (UniqueViolation).
        The final store_execution_time task uses the default global key.
        """
        Variable.set(var_key, str(kwargs["execution_date"]))
        logging.info(f"✅ Variable '{var_key}' set to {kwargs['execution_date']}")
        return

    def get_cluster_info(
        *,
        node_type_id=None,
        instance_pool_id=None,
        custom_tags=None,
        spark_version=None,
        cluster_scale=None,
        policy_id=None,
        uc=False,
        cluster_name=None,
    ):
        """
        Returns a Databricks cluster config.
        Set uc=True for Unity Catalog (UC) jobs, uc=False for non-UC jobs.
        """
        new_cluster = {
            "spark_version": spark_version,
            "custom_tags": custom_tags or {},
            "spark_conf": {
                "spark.speculation": True,
                "spark.scheduler.mode": "FAIR",
            },
            "policy_id": policy_id,
            "apply_policy_default_values": "true",
        }

        if cluster_scale:
            new_cluster.update(cluster_scale)

        if uc:
            new_cluster["data_security_mode"] = data_security_mode
            new_cluster["runtime_engine"] = "STANDARD"
            if instance_pool_id:
                new_cluster["instance_pool_id"] = instance_pool_id
                new_cluster["apply_policy_default_values"] = "false"
                new_cluster.pop("node_type_id", None)
                new_cluster.pop("driver_node_type_id", None)
                if "azure_attributes" in new_cluster:
                    new_cluster["azure_attributes"].pop("spot_bid_max_price", None)
                    if not new_cluster["azure_attributes"]:
                        del new_cluster["azure_attributes"]
            else:
                new_cluster["node_type_id"] = node_type_id
                new_cluster["apply_policy_default_values"] = "true"
        else:
            if instance_pool_id:
                new_cluster["instance_pool_id"] = instance_pool_id
            else:
                new_cluster["node_type_id"] = node_type_id
        return new_cluster

    def streaming_branch(**_context):
        if run_oms_obf_streaming_job:
            return ["dom_oms_databricks_run", "dom_obf_databricks_run"]
        else:
            return ["skip_oms_run", "skip_obf_run"]

    # ------------------------------------------------------------------------------------------
    # Check dbt silver_to_gold completion via Airflow Variable
    # ------------------------------------------------------------------------------------------
    def check_silver_to_gold_completion(**context):
        """
        Poll the Variable written by the dbt DAG after silver_to_gold succeeds.
        
        Uses data_interval_end (= the actual physical wall-clock time when Airflow
        triggers this DAG run) as the threshold. The dbt DAG stores the real UTC
        time when silver_to_gold finishes. We require that stored time to be >=
        data_interval_end so we know the dbt DAG completed AFTER this cycle started.
        
        Works for any schedule interval (30min, 1hr, etc.) because both DAGs
        share the same data_interval_end on the same schedule.
        """
        _tz = pendulum.timezone("US/Eastern")

        # data_interval_end = the REAL physical time Airflow triggered this run
        # For a 10:30→11:30 window, data_interval_end = 11:30 (actual trigger time)
        trigger_time = context["data_interval_end"]

        timeout_minutes = 120
        poke_interval_seconds = 60
        start_time = datetime.now(tz=_tz)
        deadline = start_time + timedelta(minutes=timeout_minutes)

        logging.info(
            f"Waiting for dbt_dom_silver_to_gold_CompletionTime >= {trigger_time} "
            f"(data_interval_end = physical trigger time). "
            f"Timeout: {timeout_minutes} min, poll: {poke_interval_seconds}s."
        )

        while datetime.now(tz=_tz) < deadline:
            raw = Variable.get("dbt_dom_silver_to_gold_CompletionTime", default_var=None)
            if raw:
                try:
                    completion_dt = pendulum.parse(str(raw))
                    if completion_dt >= trigger_time:
                        logging.info(
                            f"✅ silver_to_gold completed at {completion_dt} "
                            f"(>= {trigger_time}). Proceeding."
                        )
                        return
                    else:
                        logging.info(
                            f"Completion time {completion_dt} < {trigger_time}. "
                            "DBT has not finished this cycle yet. Waiting..."
                        )
                except Exception as e:
                    logging.warning(f"Parse error for value {raw!r}: {e}")
            else:
                logging.info("Variable not set yet. Waiting...")

            time.sleep(poke_interval_seconds)

        raise AirflowException(
            f"Timeout: silver_to_gold did not complete within {timeout_minutes} min. "
            f"Required >= {trigger_time}. "
            f"Last value: {Variable.get('dbt_dom_silver_to_gold_CompletionTime', default_var='NOT SET')}"
        )

    ##############################################################################################
    # ── SECTION 1: OMS / OBF Streaming  ──────────────────────
    ##############################################################################################

    faildag = PythonOperator(task_id="faildag", python_callable=failtask, dag=dag)

    # Gateway — waits only while product_master task_one is active, releases worker slot between pokes
    wait_for_product_master = ProductMasterSensor(
        task_id="wait_for_product_master",
        mode="reschedule",
        poke_interval=60,
        timeout=180 * 60,
        dag=dag,
    )

    wait_for_dbt_dag = PythonOperator(
        task_id="wait_for_dbt_dag",
        python_callable=check_silver_to_gold_completion,
        provide_context=True,
        dag=dag,
    )

    streaming_run_branch = BranchPythonOperator(
        task_id="streaming_run_branch",
        python_callable=streaming_branch,
    )

    skip_oms_run = EmptyOperator(task_id="skip_oms_run")
    skip_obf_run = EmptyOperator(task_id="skip_obf_run")

    dom_oms_databricks_run = DatabricksSubmitRunOperator(
        task_id="dom_oms_databricks_run",
        databricks_conn_id="databricks_vnet_default",
        new_cluster=get_cluster_info(
            node_type_id=oms_node_type_id,
            instance_pool_id=oms_instance_pool_id,
            custom_tags=custom_tags,
            spark_version=oms_spark_version,
            cluster_scale=oms_cluster_scale,
            policy_id=oms_policy_id,
            uc=False,
        ),
        json={
            "run_name": "dbx_dom_global_daily_oms_run_{{ ds }}",
            "spark_jar_task": {
                "main_class_name": oms_main_class,
                "parameters": oms_params,
            },
            "libraries": [{"jar": oms_jar_path}],
        },
        timeout_seconds=3600,
        polling_period_seconds=10,
        wait_for_termination=True,
    )

    dom_obf_databricks_run = DatabricksSubmitRunOperator(
        task_id="dom_obf_databricks_run",
        databricks_conn_id="databricks_vnet_default",
        new_cluster=get_cluster_info(
            node_type_id=obf_node_type_id,
            instance_pool_id=obf_instance_pool_id,
            custom_tags=custom_tags,
            spark_version=obf_spark_version,
            cluster_scale=obf_cluster_scale,
            policy_id=obf_policy_id,
            uc=False,
        ),
        json={
            "run_name": "dbx_dom_global_daily_obf_run_{{ ds }}",
            "spark_jar_task": {
                "main_class_name": obf_main_class,
                "parameters": obf_params,
            },
            "libraries": [{"jar": obf_jar_path}],
        },
        timeout_seconds=3600,
        polling_period_seconds=10,
        wait_for_termination=True,
    )

    # Convergence point — succeeds when at least one streaming branch finishes without failure
    streaming_done = EmptyOperator(
        task_id="streaming_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    ##############################################################################################
    # ── SECTION 2: Dynamic Databricks Scala JAR Jobs ───
    ##############################################################################################

    # Gate that opens once streaming_done succeeds — fans out to per-table-group tasks
    dom_databricks_group_start = EmptyOperator(
        task_id="dom_databricks_group_start",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # --- Dynamic Databricks tasks per table group ---
    dom_table_groups = dom_json.get("dom_table_groups", [])
    dom_databricks_tasks = []
    dom_store_execution_tasks = []
    only_all = len(dom_table_groups) == 1 and dom_table_groups[0] == ["all"]

    for group in dom_table_groups:
        group_tables = group if isinstance(group, list) else [group]
        group_str = ",".join(group_tables)
        group_task_id = "_".join(group_tables)

        # Per-table loadType support
        table_load_types = dom_json.get("tableLoadTypes", {})
        table_load_types_json = json.dumps(table_load_types) if table_load_types else ""
        load_type = dom_json.get("loadType", "INCREMENTAL")

        params = [
            dom_json.get("databricks"),
            dom_json.get("delta_initial_path"),
            dom_json.get("exchange_landing_table"),
            dom_json.get("obf_order_history_table"),
            dom_json.get("refund_refined_table"),
            dom_json.get("refund_landing_table"),
            dom_json.get("return_refined_table"),
            dom_json.get("return_landing_table"),
            dom_json.get("order_refined_table"),
            dom_json.get("order_landing_table"),
            dom_json.get("consignments_refined_table"),
            dom_json.get("consignments_landing_table"),
            dom_json.get("oms_parsed_tender_table"),
            load_type,
            dom_json.get("SnowflakeDBScope"),
            dom_json.get("varSubstitutions"),
            table_load_types_json,
            group_str,
        ]

        task = DatabricksSubmitRunOperator(
            task_id=f"dom_{group_task_id}_run",
            databricks_conn_id="databricks_vnet_default",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            new_cluster=get_cluster_info(
                node_type_id=node_type_id,
                instance_pool_id=instance_pool_id,
                custom_tags=custom_tags,
                spark_version=spark_version,
                cluster_scale=cluster_scale,
                policy_id=policy_id,
                uc=True,
            ),
            json={
                "run_name": f"dbx_dom_global_daily_{group_task_id}_run_{{{{ ds }}}}",
                "spark_jar_task": {
                    "main_class_name": main_class,
                    "parameters": params,
                },
                "libraries": [{"jar": jar_path}],
            },
            timeout_seconds=3600,
            retries=2,
            retry_delay=timedelta(minutes=1),
            polling_period_seconds=10,
            wait_for_termination=True,
        )
        dom_databricks_tasks.append(task)

        if not only_all:
            exec_task = PythonOperator(
                task_id=f"store_execution_{group_task_id}",
                provide_context=True,
                python_callable=python_method,
                op_kwargs={"var_key": f"dbx_dom_{group_task_id}_ExecutionTime"},
                dag=dag,
            )
            dom_store_execution_tasks.append(exec_task)

    # Final convergence before completion
    databricks_done = EmptyOperator(
        task_id="databricks_done",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    store_execution_time = PythonOperator(
        task_id="store_execution_time",
        provide_context=True,
        python_callable=python_method,
        dag=dag,
    )

    ##############################################################################################
    # Dependencies
    ##############################################################################################

    # ── OMS / OBF Streaming ─────────────────────────────────────────────────────────────────
    streaming_run_branch >> [dom_oms_databricks_run, skip_oms_run, dom_obf_databricks_run, skip_obf_run]
    [dom_oms_databricks_run, skip_oms_run, dom_obf_databricks_run, skip_obf_run] >> streaming_done

    # ── Bridge to Databricks Scala JAR section ───────────────────────────────────────────────
    streaming_done >> wait_for_product_master >> dom_databricks_group_start
    wait_for_dbt_dag >> dom_databricks_group_start
    # [wait_for_product_master, wait_for_dbt_dag] >> dom_databricks_group_start

    # ── Dynamic per-table-group tasks ───────────────────────────────────────────────────────
    for t in dom_databricks_tasks:
        dom_databricks_group_start >> t

    # Fan-in with optional per-group execution tracking
    if only_all:
        for t in dom_databricks_tasks:
            t >> databricks_done
    else:
        if len(dom_store_execution_tasks) == len(dom_databricks_tasks):
            for t, exec_t in zip(dom_databricks_tasks, dom_store_execution_tasks):
                t >> exec_t >> databricks_done
        else:
            for t in dom_databricks_tasks:
                t >> databricks_done

    databricks_done >> store_execution_time

    # ── Failure handling ────────────────────────────────────────────────────────────────────
    databricks_done >> job_failure_notification >> faildag

# End of DAG file