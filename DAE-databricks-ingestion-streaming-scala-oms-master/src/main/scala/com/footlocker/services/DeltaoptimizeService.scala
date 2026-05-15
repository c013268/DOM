package com.footlocker.services
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.util.Calendar

trait DeltaoptimizeService {

  def optimize(spark: SparkSession, jobname: String): Unit = {
    val dT = Calendar.getInstance()
    val now = Calendar.getInstance().getTime
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    val currentHour = dT.get(Calendar.HOUR_OF_DAY)

    val dowInt = new SimpleDateFormat("u")
    val currentDay = dowInt.format(now)
    val today = formatter.format(dT.getTime)
    println(jobname, "before", today)
    var JobName = jobname
    var TableName = ""
    var TableFlag = ""
    var VacuumFlag = ""
    var OptimizeFlag = ""
    var RepairFlag = ""
    var VacuumHour = 0
    var VacuumDay = 0
    var OptimizeHour = 0
    var OptimizeDay = 0
    var RepairHour = 0
    var RepairDay = 0
    var Retention = ""

    val JobConfigDF = spark.sql(s"select * from etl_stats.StreamingJobVacuumConfig where JobName='$JobName'")

    for (row <- JobConfigDF.rdd.collect) {
      JobName = row.mkString(",").split(",")(0)
      TableName = row.mkString(",").split(",")(1)
      TableFlag = row.mkString(",").split(",")(2)
      VacuumFlag = row.mkString(",").split(",")(3)
      OptimizeFlag = row.mkString(",").split(",")(4)
      RepairFlag = row.mkString(",").split(",")(5)
      VacuumHour = row.mkString(",").split(",")(6).toInt
      VacuumDay = row.mkString(",").split(",")(7).toInt
      OptimizeHour = row.mkString(",").split(",")(8).toInt
      OptimizeDay = row.mkString(",").split(",")(9).toInt
      RepairHour = row.mkString(",").split(",")(10).toInt
      RepairDay = row.mkString(",").split(",")(11).toInt
      Retention = row.mkString(",").split(",")(12)
      // var LastOptimizeDate=row.mkString(",").split(",")(13)
      // var LastVaccumDate=row.mkString(",").split(",")(14)
      // var LastRepairDate=row.mkString(",").split(",")(15)


      if (OptimizeFlag == "Y") {


        if (OptimizeDay == 100 || currentDay.toInt == OptimizeDay.toInt) {


          if (OptimizeHour == currentHour) {

            val OptimizeStart = LocalDateTime.now()
            if (TableFlag == "Y") {

              spark.sql(s"""insert into  etl_stats.StreamingJobVacuumLog values('$JobName','$TableName','Optimize','$OptimizeStart',null)""")
              spark.sql(s"""optimize $TableName""")
              spark.sql(s"""update etl_stats.StreamingJobVacuumConfig set LastOptimizeDate=Current_Timestamp where JobName='$JobName' and TableName='$TableName'""")
              spark.sql(s"""update etl_stats.StreamingJobVacuumLog set CleanupActionTypeEndTime=Current_Timestamp where CleanupActionType='Optimize' and  CleanupActionTypeStartTime='$OptimizeStart' and JobName='$JobName' and TableName='$TableName'""")
            }
            else {
              spark.sql(s"""insert into  etl_stats.StreamingJobVacuumLog values('$JobName','$TableName','Optimize','$OptimizeStart',null)""")
              spark.sql(s"""OPTIMIZE delta.`$TableName`  """)
              spark.sql(s"""update etl_stats.StreamingJobVacuumConfig set LastOptimizeDate=Current_Timestamp where JobName='$JobName' and TableName='$TableName'""")
              spark.sql(s"""update etl_stats.StreamingJobVacuumLog set CleanupActionTypeEndTime=Current_Timestamp where CleanupActionType='Optimize' and CleanupActionTypeStartTime='$OptimizeStart' and JobName='$JobName' and TableName='$TableName'""")
            }


          }
        }

      }
      if (VacuumFlag == "Y") {
        if (VacuumDay == 100 || currentDay.toInt == VacuumDay.toInt) {

          val VaccumStart = LocalDateTime.now()
          if (VacuumHour == currentHour) {
            if (TableFlag == "Y") {
              spark.sql(s"""insert into  etl_stats.StreamingJobVacuumLog values('$JobName','$TableName','Vacuum','$VaccumStart',null)""")
              spark.sql(s"""VACUUM $TableName""")
              spark.sql(s"""update etl_stats.StreamingJobVacuumConfig set LastVaccumDate=Current_Timestamp where JobName='$JobName' and TableName='$TableName'""")
              spark.sql(s"""update etl_stats.StreamingJobVacuumLog set CleanupActionTypeEndTime=Current_Timestamp where CleanupActionType='Vacuum'  and CleanupActionTypeStartTime='$VaccumStart' and  JobName='$JobName' and TableName='$TableName'""")
            }
            else {
              spark.sql(s"""insert into  etl_stats.StreamingJobVacuumLog values('$JobName','$TableName','Vacuum','$VaccumStart',null)""")
              spark.sql(s"VACUUM delta.`$TableName` ")
              spark.sql(s"""update etl_stats.StreamingJobVacuumConfig set LastVaccumDate=Current_Timestamp where JobName='$JobName' and TableName='$TableName'""")
              spark.sql(s"""update etl_stats.StreamingJobVacuumLog set CleanupActionTypeEndTime=Current_Timestamp where CleanupActionType='Vacuum' and  CleanupActionTypeStartTime='$VaccumStart' and JobName='$JobName' and TableName='$TableName'""")

            }

          }
        }

      }

      if (RepairFlag == "Y") {
        if (RepairDay == 100 || currentDay.toInt == RepairDay.toInt) {

          val RepairStart = LocalDateTime.now()
          if (RepairHour == currentHour) {
            if (TableFlag == "Y") {
              spark.sql(s"""insert into  etl_stats.StreamingJobVacuumLog values('$JobName','$TableName','Repair','$RepairStart',null)""")
              spark.sql(s"""fsck repair table $TableName""")
              spark.sql(s"""update etl_stats.StreamingJobVacuumConfig set LastRepairDate=Current_Timestamp where JobName='$JobName' and TableName='$TableName'""")
              spark.sql(s"""update etl_stats.StreamingJobVacuumLog set CleanupActionTypeEndTime=Current_Timestamp where CleanupActionType='Repair' and  CleanupActionTypeStartTime='$RepairStart' and JobName='$JobName' and TableName='$TableName'""")

            }

          }
        }

      }


    }


  }
}