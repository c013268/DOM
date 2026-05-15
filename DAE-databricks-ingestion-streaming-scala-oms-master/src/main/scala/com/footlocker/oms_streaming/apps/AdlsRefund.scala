package com.footlocker.oms_streaming.apps

import com.footlocker.conf.AppConfObject.Options
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.footlocker.services.SnowflakeHelperService
import com.footlocker.utils.Common.{applyschema}
import com.footlocker.services.DeltaoptimizeService


object AdlsRefund extends SnowflakeHelperService {


  def refund_adls_push(oms_refund_input: DataFrame,spark:SparkSession,inputParams : Map[String, String]): Unit = {
    oms_refund_input.sparkSession.conf.set("spark.sql.shuffle.partitions", 1)
    oms_refund_input.sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    oms_refund_input.sparkSession.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

    import spark.implicits._

    //    val oms_refund_raw = oms_refund_input.withColumn("REFUND_flag", when($"json".contains("""messageType":"REFUND"""), "1").when($"json".contains("""messageType" : "REFUND"""), "1").otherwise("0"))
    //      .filter("REFUND_flag == 1")
    //      .drop("REFUND_flag")

    val schemapath:String = (inputParams("schema_location")+"/refunds.json")
    val parsed = applyschema(spark,oms_refund_input,schemapath)

    val oms_refund_latest = parsed.select("oms_parsed_data.orderId",
      "oms_parsed_data.refundId",
      "oms_parsed_data.order",
      "oms_parsed_data.postedAt",
      "oms_parsed_data.postedBy",
      "oms_parsed_data.refundStatus",
      "load_time_kafka",
      "load_time_adls"
    )

    val refund_explode_1 = oms_refund_latest.select(
      trim(oms_refund_latest("orderId")) as "order_id",
      trim(oms_refund_latest("refundId")) as "refundId",
      oms_refund_latest("order") as "order",
      explode_outer(oms_refund_latest("order.refunds")).alias("refunds"),
      trim(oms_refund_latest("postedAt")) as "postedAt",
      trim(oms_refund_latest("postedBy")) as "postedBy",
      $"refundStatus",
      $"load_time_kafka",
      $"load_time_adls"
    )

    val refund_explode_2 = refund_explode_1.select(
      $"order_id",
      $"refundId",
      $"order",
      $"refunds",
      explode_outer($"refunds.lines") as "lines",
      $"postedAt",
      $"postedBy",
      $"refundStatus",
      $"load_time_kafka",
      $"load_time_adls"
    )


    val refunds_parsed = refund_explode_2.select(
      $"order_id",
      trim($"order.orderHeader.orderDateTime") as "order_datetime",
      $"refundId",
      $"refunds.companyNumber" as "companyNumber",
      $"refunds.date" as "refund_header_date",
      $"refunds.status" as "refund_status",
      $"refunds.createdBy" as "refund_createdBy",
      $"refunds.userId" as "refund_userId",
      $"refunds.source" as "source",
      $"refunds.refundNum" as "refundNum",
      $"lines.quantity" as "quantity",
      $"lines.product.name" as "name",
      $"lines.product.sku" as "sku",
      $"lines.product.size" as "size",
      $"lines.product.color" as "color",
      $"lines.product.image" as "image",
      $"lines.product.brand" as "brand",
      $"lines.product.category" as "category",
      $"lines.product.description" as "description",
      $"lines.product.isCollectUpFront" as "isCollectUpFront",
      $"lines.product.backorderFlag" as "backorderFlag",
      $"lines.product.launchSkuFlag" as "launchSkuFlag",
      $"lines.product.taxCode" as "taxCode",
      $"lines.product.productDesignator" as "productDesignator",
      $"lines.product.productNumber" as "productNumber",
      $"lines.product.productType" as "productType",
      $"lines.cegrRefId" as "cegrRefId",
      $"lines.requestedLinePrice.originalRetailPrice" as "originalRetailPrice",
      $"lines.requestedLinePrice.originalUnitPrice" as "originalUnitPrice",
      $"lines.requestedLinePrice.originalUnitDiscountAmount" as "originalUnitDiscountAmount",
      $"lines.requestedLinePrice.lineRefundSubTotal" as "lineRefundSubTotal",
      $"lines.requestedLinePrice.taxAmount" as "taxAmount",
      $"lines.requestedLinePrice.shippingAmount" as "shippingAmount",
      $"lines.requestedLinePrice.shippingTaxAmount" as "shippingTaxAmount",
      $"lines.requestedLinePrice.inboundShippingAmount" as "inboundShippingAmount",
      $"lines.requestedLinePrice.totalAmount" as "totalAmount",
      $"lines.returnExpected" as "returnExpected",
      $"lines.returned" as "returned",
      $"lines.returnNumbers" as "returnNumbers",
      $"lines.quantityReturned" as "quantityReturned",
      $"lines.returnDate" as "returnDate",
      $"lines.reasonCode" as "reasonCode",
      $"refunds.requestedTotals.inboundShippingAmount" as "ih_inboundShippingAmount",
      $"refunds.requestedTotals.lineRefundSubTotal" as "ih_lineRefundSubTotal",
      $"refunds.requestedTotals.originalRetailPrice" as "ih_originalRetailPrice",
      $"refunds.requestedTotals.originalUnitDiscountAmount" as "ih_originalUnitDiscountAmount",
      $"refunds.requestedTotals.originalUnitPrice" as "ih_originalUnitPrice",
      $"refunds.requestedTotals.shippingAmount" as "ih_shippingAmount",
      $"refunds.requestedTotals.shippingTaxAmount" as "ih_shippingTaxAmount",
      $"refunds.requestedTotals.taxAmount" as "ih_taxAmount",
      $"refunds.requestedTotals.totalAmount" as "ih_totalAmount",
      $"refunds.refundMethods" as "refundMethods",
      $"refunds.paymentsInfo" as "paymentsInfo",
      $"refunds.notes" as "refund_notes",
      $"refunds.statusHistory" as "refunds_statusHistory",
      $"load_time_kafka",
      $"load_time_adls",
      to_date(trim($"order.orderHeader.orderDateTime")) as "order_date",
      $"postedAt",
      $"postedBy"
    )

    // refunds_parsed.persist()
    refunds_parsed.write.format("delta").mode("append").partitionBy("order_date").save(inputParams("adls_url")+"/landing/sales/oms/refunds")

    writeSnowflakeTable(Options,
      "REFUNDS",
      refunds_parsed,
      SaveMode.Append)

    val partitionWindow = Window.partitionBy($"companyNumber",$"order_id",$"sku",$"size",$"refundId",$"refundNum").orderBy($"postedAt".desc)
    val rownumber = row_number().over(partitionWindow)
    val refunds_refined = refunds_parsed.select($"*", rownumber as "rnum").filter($"rnum" === "1")
      .drop("rnum")
      .withColumn("updated_datetime",col("load_time_adls"))
    refunds_refined.createOrReplaceTempView("refunds_refined")

    //    val mergequery = Source.fromInputStream(AdlsRefund.getClass.getResourceAsStream("/sqls/mergerefunds.sql")).mkString
    //    snowflakequeryrun(mergequery,Options)

    oms_refund_input.sparkSession.sql(s"""
          MERGE INTO sales.oms_refunds t
          USING refunds_refined s
          ON s.order_id = t.order_id AND s.sku = t.sku and t.companyNumber = s.companyNumber
          and s.size = t.size and s.refundId = t.refundId and s.refundNum =t.refundNum
          WHEN MATCHED THEN UPDATE SET
          t.refund_header_date = s.refund_header_date ,
          t.refund_status = s.refund_status ,
          t.refund_createdBy = s.refund_createdBy,
          t.refund_userId = s.refund_userId,
          t.source = s.source ,
          t.quantity = s.quantity ,
          t.name = s.name ,
          t.color = s.color ,
          t.image = s.image,
          t.brand = s.brand ,
          t.category = s.category ,
          t.description = s.description ,
          t.isCollectUpFront = s.isCollectUpFront ,
          t.backorderFlag = s.backorderFlag ,
          t.launchSkuFlag = s.launchSkuFlag ,
          t.taxCode = s.taxCode ,
          t.productDesignator = s.productDesignator ,
          t.productNumber = s.productNumber ,
          t.productType = s.productType ,
          t.cegrRefId = s.cegrRefId,
          t.originalRetailPrice = s.originalRetailPrice ,
          t.originalUnitPrice = s.originalUnitPrice ,
          t.originalUnitDiscountAmount = s.originalUnitDiscountAmount ,
          t.lineRefundSubTotal = s.lineRefundSubTotal ,
          t.taxAmount = s.taxAmount ,
          t.shippingAmount = s.shippingAmount ,
          t.shippingTaxAmount = s.shippingTaxAmount ,
          t.inboundShippingAmount = s.inboundShippingAmount ,
          t.totalAmount = s.totalAmount ,
          t.returnExpected = s.returnExpected ,
          t.returned = s.returned ,
          t.returnNumbers = s.returnNumbers,
          t.quantityReturned = s.quantityReturned ,
          t.returnDate = s.returnDate ,
          t.reasonCode = s.reasonCode ,
          t.ih_inboundShippingAmount = s.ih_inboundShippingAmount ,
          t.ih_lineRefundSubTotal = s.ih_lineRefundSubTotal ,
          t.ih_originalRetailPrice = s.ih_originalRetailPrice ,
          t.ih_originalUnitDiscountAmount = s.ih_originalUnitDiscountAmount ,
          t.ih_originalUnitPrice = s.ih_originalUnitPrice ,
          t.ih_shippingAmount = s.ih_shippingAmount ,
          t.ih_shippingTaxAmount = s.ih_shippingTaxAmount ,
          t.ih_taxAmount = s.ih_taxAmount ,
          t.ih_totalAmount = s.ih_totalAmount ,
          t.refundMethods = s.refundMethods,
          t.paymentsInfo = s.paymentsInfo,
          t.refund_notes = s.refund_notes,
          t.refunds_statusHistory = s.refunds_statusHistory,
          t.load_time_kafka = s.load_time_kafka ,
          t.postedAt = s.postedAt ,
          t.updated_datetime = s.load_time_adls
          WHEN NOT MATCHED THEN INSERT *
        """)

    //refunds_parsed.unpersist()

  }







}
