###############################################################################################
# DAG ID: dbt_dom_global_daily
# Description: DBT Cloud pipeline — batch start, stage -> bronze -> silver -> gold -> refactored ->
#              batch end, plus execution-time tracking.
#
# Author: Pavan Kalyan Kotha
# Last Updated: May 03, 2026
###############################################################################################

import logging
import time
import requests
from datetime import datetime, timedelta
from json import dumps, loads

import pendulum
from airflow.exceptions import AirflowException

from airflow.models import DAG, Variable, DagRun, DagModel, TaskInstance
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.session import create_session
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
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

dag_id = "dbt_dom_global_daily"
local_tz = pendulum.timezone("US/Eastern")

dom_json = Variable.get("dom_prod_json", deserialize_json=True)
schedule_interval = dom_json["schedule_interval"]
dbt_default_args = dom_json.get("dbt_default_args", {})
dom_dbt_job_id_batch_start = dom_json.get("dom_dbt_job_id_batch_start")
dom_dbt_job_id_priority_journal_to_bronze = dom_json.get("dom_dbt_job_id_journal_to_bronze")
dom_dbt_job_id_priority_stage_to_silver = dom_json.get("dom_dbt_job_id_stage_to_silver")
dom_dbt_job_id_non_priority_journal_to_bronze = dom_json.get("dom_dbt_journal_to_bronze_nonpriority")
dom_dbt_job_id_non_priority_stage_to_silver = dom_json.get("dom_dbt_stage_to_silver_nonpriority")
dom_dbt_job_id_bronze_to_silver_hist = dom_json.get("dom_dbt_job_id_bronze_to_silver_hist")
dom_dbt_job_id_silver_to_gold = dom_json.get("dom_dbt_job_id_silver_to_gold")
dom_dbt_job_id_gold_to_dbimart_refactored = dom_json.get("dom_dbt_job_id_gold_to_dbimart_refactored")
dom_dbt_job_id_post_gold_dq = dom_json.get("dom_dbt_job_id_post_gold_dq")
dom_dbt_job_id_batch_end = dom_json.get("dom_dbt_job_id_batch_end")
non_priority_run_hour = dom_json.get("dom_non_priority_hour")
max_retries = 2

##############################################################################################
# DAG Definition
##############################################################################################

with DAG(
    dag_id=dag_id,
    description="Daily DOM DBT pipeline: stage -> bronze -> silver -> gold -> Refactored -> batch end",
    default_args=dbt_default_args,
    schedule_interval=schedule_interval,
    start_date=datetime(2024, 1, 25, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    tags=["dom", "dbt", "uc"],
) as dag:

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
        """Pauses the DAG to prevent deadlock, then raises to mark the run as failed."""
        from airflow.utils.session import create_session

        logging.warning(f"A task has failed. Pausing DAG '{dag_id}' to prevent deadlock.")

        try:
            with create_session() as session:
                dag_model = session.query(DagModel).filter(DagModel.dag_id == dag_id).first()
                if dag_model:
                    dag_model.is_paused = True
                    session.commit()
                    logging.info(f"DAG '{dag_id}' has been PAUSED. No new runs will be scheduled.")
                    logging.info("To resume: fix the issue, clear the failed task, then unpause the DAG.")
                else:
                    logging.error(f"Could not find DagModel for '{dag_id}'.")
        except Exception as e:
            logging.error(f"Failed to pause DAG: {e}")

        raise ValueError("DAG Failure")

    def python_method(ds, **kwargs):
        Variable.set("dbt_dom_global_daily_ExecutionTime", kwargs["execution_date"])
        return

    def record_silver_to_gold_completion(**context):
        """Stamp the current UTC time so downstream DAGs can verify freshness."""
        now_utc = pendulum.now("UTC").to_iso8601_string()
        Variable.set("dbt_dom_silver_to_gold_CompletionTime", now_utc)
        logging.info(f"Recorded silver_to_gold completion time: {now_utc}")

    def dom_non_priority_branch(**context):
        dag_run = context.get("dag_run")
        logical_date = context.get("logical_date")

        if not dag_run or not logical_date:
            logging.info("No dag_run or logical_date found. Skipping non-priority.")
            return "skip_non_priority"

        run_type = getattr(dag_run, "run_type", None)
        logging.info(f"DAG run_type: {run_type}")

        if run_type and str(run_type).lower() != "scheduled":
            logging.info("Non-scheduled run detected. Skipping non-priority.")
            return "skip_non_priority"

        data_interval_end = context["data_interval_end"].astimezone(local_tz)

        logging.info(f"Data interval end (localized): {data_interval_end}")
        logging.info(f"Run hour: {data_interval_end.hour}")
        logging.info(f"Configured non-priority run hour: {non_priority_run_hour}")

        if data_interval_end.hour == non_priority_run_hour:
            logging.info("✅ Non-priority window matched. Running non-priority tasks.")
            return [
                "dom_journal_to_bronze_non_priority",
                "dom_stage_to_silver_non_priority",
            ]
        else:
            logging.info("⏭ Outside non-priority window. Skipping non-priority tasks.")
            return "skip_non_priority"

    def trigger_powerbi_refresh(**context):
        """Trigger Power BI dataset refreshes after gold layer completion."""
        pbi_config = dom_json.get("powerbi_config", {})
        
        if not pbi_config.get("enabled", False):
            logging.info("Power BI refresh is disabled in config.")
            return
        
        # Get credentials from Airflow Variables
        client_id = pbi_config["client_id_airflow_var"]
        client_secret = pbi_config["client_secret_airflow_var"]
        tenant_id = pbi_config["tenant_id"]
        
        # 1. Get Access Token
        logging.info("Acquiring Power BI access token...")
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        
        try:
            token_resp = requests.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "resource": "https://analysis.windows.net/powerbi/api"
                },
                timeout=30
            )
            token_resp.raise_for_status()
            token = token_resp.json()["access_token"]
            logging.info("Token acquired successfully.")
        except Exception as e:
            logging.error(f"Failed to acquire Power BI token: {e}")
            raise
        
        # 2. Trigger Refreshes
        datasets = pbi_config.get("datasets", [])
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Get current hour in EST for time-window filtering
        _tz = pendulum.timezone("US/Eastern")
        current_hour = datetime.now(tz=_tz).hour
        logging.info(f"Current EST hour: {current_hour}")

        refresh_results = []
        for ds in datasets:
            group_id = ds.get("group_id")
            dataset_id = ds.get("dataset_id")
            name = ds.get("name", "Unknown")
            
            # Skip empty ones
            if not group_id or not dataset_id:
                logging.warning(f"Skipping {name} - missing group_id or dataset_id")
                continue

            # ── Time-window check ──
            # Each dataset can define min_hour / max_hour in config.
            # Refresh only runs when: current_hour > min_hour AND current_hour < max_hour
            # Examples:
            #   OpenOrders / NewDataset: min_hour=-1, max_hour=24 → always refreshes (24/7)
            #   Zendesk:                 min_hour=5,  max_hour=18 → refreshes 6:00 AM – 5:59 PM EST
            min_hour = ds.get("min_hour", -1)
            max_hour = ds.get("max_hour", 24)

            if not (current_hour > min_hour and current_hour < max_hour):
                logging.info(
                    f"⏭️ [{name}] Skipped — current hour {current_hour} EST is outside "
                    f"allowed window (>{min_hour} and <{max_hour})."
                )
                refresh_results.append({"dataset": name, "status": "skipped_time_window"})
                continue
            
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes"
            
            logging.info(f"Triggering refresh for [{name}]...")
            try:
                resp = requests.post(url, headers=headers, timeout=60)
                
                if resp.status_code == 202:
                    logging.info(f"✅ [{name}] Refresh accepted (202)")
                    refresh_results.append({"dataset": name, "status": "accepted"})
                elif resp.status_code == 400:
                    logging.info(f"⏭️ [{name}] Refresh already in progress (400)")
                    refresh_results.append({"dataset": name, "status": "already_running"})
                else:
                    logging.error(f"❌ [{name}] Failed with status {resp.status_code}: {resp.text}")
                    refresh_results.append({"dataset": name, "status": "failed", "error": resp.text})
                    # Optional: uncomment to fail the task on any PBI error
                    # resp.raise_for_status()
            except Exception as e:
                logging.error(f"Exception triggering refresh for [{name}]: {e}")
                refresh_results.append({"dataset": name, "status": "error", "error": str(e)})
        
        logging.info(f"Power BI refresh summary: {refresh_results}")
        return refresh_results

    ##############################################################################################
    # Tasks
    ##############################################################################################
    # Gateway — waits only while product_master task_one is active, releases worker slot between pokes
    wait_for_product_master = ProductMasterSensor(
        task_id="wait_for_product_master",
        mode="reschedule",
        poke_interval=60,
        timeout=180 * 60,
        dag=dag,
    )

    faildag = PythonOperator(
        task_id="faildag",
        python_callable=failtask,
        dag=dag,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    dom_batch_start = DbtCloudRunJobOperator(
        task_id="dom_batch_start",
        job_id=dom_dbt_job_id_batch_start,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    dom_journal_to_bronze_priority = DbtCloudRunJobOperator(
        task_id="dom_journal_to_bronze_priority",
        job_id=dom_dbt_job_id_priority_journal_to_bronze,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    dom_stage_to_silver_priority = DbtCloudRunJobOperator(
        task_id="dom_stage_to_silver_priority",
        job_id=dom_dbt_job_id_priority_stage_to_silver,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    dom_journal_to_bronze_non_priority = DbtCloudRunJobOperator(
        task_id="dom_journal_to_bronze_non_priority",
        job_id=dom_dbt_job_id_non_priority_journal_to_bronze,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    dom_stage_to_silver_non_priority = DbtCloudRunJobOperator(
        task_id="dom_stage_to_silver_non_priority",
        job_id=dom_dbt_job_id_non_priority_stage_to_silver,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    dom_bronze_to_silver_hist = DbtCloudRunJobOperator(
        task_id="dom_bronze_to_silver_hist",
        job_id=dom_dbt_job_id_bronze_to_silver_hist,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    dom_silver_to_gold = DbtCloudRunJobOperator(
        task_id="dom_silver_to_gold",
        job_id=dom_dbt_job_id_silver_to_gold,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    record_silver_to_gold_time = PythonOperator(
        task_id="record_silver_to_gold_time",
        python_callable=record_silver_to_gold_completion,
        provide_context=True,
        dag=dag,
    )

    dom_post_gold_dq = DbtCloudRunJobOperator(
        task_id="dom_post_gold_dq",
        job_id=dom_dbt_job_id_post_gold_dq,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    dom_gold_to_dbimart_refactored = DbtCloudRunJobOperator(
        task_id="dom_gold_to_dbimart_refactored",
        job_id=dom_dbt_job_id_gold_to_dbimart_refactored,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    powerbi_refresh = PythonOperator(
        task_id="powerbi_refresh",
        python_callable=trigger_powerbi_refresh,
        provide_context=True,
        dag=dag,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
    )

    dom_batch_end = DbtCloudRunJobOperator(
        task_id="dom_batch_end",
        dag=dag,
        job_id=dom_dbt_job_id_batch_end,
        check_interval=10,
        wait_for_termination=True,
        retries=max_retries,
        retry_delay=timedelta(minutes=1),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    non_priority_branch = BranchPythonOperator(
        task_id="non_priority_branch",
        python_callable=dom_non_priority_branch,
    )

    skip_non_priority = EmptyOperator(task_id="skip_non_priority")

    store_execution_time = PythonOperator(
        task_id="store_execution_time",
        provide_context=True,
        python_callable=python_method,
        dag=dag,
    )

    ##############################################################################################
    # Dependencies
    ##############################################################################################

    # Non-priority path
    dom_batch_start >> non_priority_branch
    non_priority_branch >> dom_journal_to_bronze_non_priority >> dom_stage_to_silver_non_priority >> dom_post_gold_dq >> dom_batch_end
    non_priority_branch >> skip_non_priority

    # Priority path
    dom_batch_start >> dom_journal_to_bronze_priority >> [dom_bronze_to_silver_hist, dom_stage_to_silver_priority] >> dom_silver_to_gold >> wait_for_product_master >> dom_gold_to_dbimart_refactored >> powerbi_refresh
    # wait_for_product_master >> dom_gold_to_dbimart_refactored
    dom_silver_to_gold >> record_silver_to_gold_time >> dom_batch_end
    # dom_silver_to_gold >> dom_batch_end

    # Post batch-end
    dom_batch_end >> store_execution_time

    # Failure handling
    dom_batch_end >> job_failure_notification
    dom_batch_end >> faildag

# End of DAG file