package com.footlocker.oms_streaming.apps

import com.databricks.dbutils_v1.DBUtilsHolder.dbutils
import com.footlocker.conf.AppConfObject_Gen2.getSnowflakeConnectionOptions
import com.footlocker.services.SnowflakeHelperService_Gen2
import com.footlocker.utils.Common_Gen2.applyschema
import com.footlocker.utils.Constants.AppendGen2ToSnowFlakeTablesFlag
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object AdlsRefund_Gen2 extends SnowflakeHelperService_Gen2 {


  def refund_adls_push(oms_refund_input: DataFrame, spark: SparkSession, inputParams: Map[String, String]): Unit = {
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
      $"lines.lineNumber" as "lineNumber",
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
    refunds_parsed.withColumn("lineNumber", col("lineNumber").cast("String")).write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("order_date").save(inputParams("pii_landing_path")+ "/sales/oms/refunds")

    // Select columns in exact DDL order for Snowflake write
    val snowflakeCols = Seq(
      "ORDER_ID", "ORDER_DATETIME", "REFUNDID", "COMPANYNUMBER", "REFUND_HEADER_DATE", "REFUND_STATUS", "REFUND_CREATEDBY", "REFUND_USERID", "SOURCE", "REFUNDNUM", "QUANTITY", "NAME", "SKU", "SIZE", "COLOR", "IMAGE", "BRAND", "CATEGORY", "DESCRIPTION", "ISCOLLECTUPFRONT", "BACKORDERFLAG", "LAUNCHSKUFLAG", "TAXCODE", "PRODUCTDESIGNATOR", "PRODUCTNUMBER", "PRODUCTTYPE", "CEGRREFID", "ORIGINALRETAILPRICE", "ORIGINALUNITPRICE", "ORIGINALUNITDISCOUNTAMOUNT", "LINEREFUNDSUBTOTAL", "TAXAMOUNT", "SHIPPINGAMOUNT", "SHIPPINGTAXAMOUNT", "INBOUNDSHIPPINGAMOUNT", "TOTALAMOUNT", "RETURNEXPECTED", "RETURNED", "RETURNNUMBERS", "QUANTITYRETURNED", "RETURNDATE", "REASONCODE", "IH_INBOUNDSHIPPINGAMOUNT", "IH_LINEREFUNDSUBTOTAL", "IH_ORIGINALRETAILPRICE", "IH_ORIGINALUNITDISCOUNTAMOUNT", "IH_ORIGINALUNITPRICE", "IH_SHIPPINGAMOUNT", "IH_SHIPPINGTAXAMOUNT", "IH_TAXAMOUNT", "IH_TOTALAMOUNT", "REFUNDMETHODS", "PAYMENTSINFO", "REFUND_NOTES", "REFUNDS_STATUSHISTORY", "LOAD_TIME_KAFKA", "LOAD_TIME_ADLS", "ORDER_DATE", "POSTEDAT", "POSTEDBY", "LINENUMBER", "REF_SOURCE"
    )
    val refunds_parsed_snow = refunds_parsed
      .withColumn("REF_SOURCE", lit("OMS"))
      .withColumnRenamed("companyNumber", "COMPANYNUMBER")
      .withColumnRenamed("refund_header_date", "REFUND_HEADER_DATE")
      .withColumnRenamed("refund_status", "REFUND_STATUS")
      .withColumnRenamed("refund_createdBy", "REFUND_CREATEDBY")
      .withColumnRenamed("refund_userId", "REFUND_USERID")
      .withColumnRenamed("source", "SOURCE")
      .withColumnRenamed("refundNum", "REFUNDNUM")
      .withColumnRenamed("quantity", "QUANTITY")
      .withColumnRenamed("name", "NAME")
      .withColumnRenamed("sku", "SKU")
      .withColumnRenamed("size", "SIZE")
      .withColumnRenamed("color", "COLOR")
      .withColumnRenamed("image", "IMAGE")
      .withColumnRenamed("brand", "BRAND")
      .withColumnRenamed("category", "CATEGORY")
      .withColumnRenamed("description", "DESCRIPTION")
      .withColumnRenamed("isCollectUpFront", "ISCOLLECTUPFRONT")
      .withColumnRenamed("backorderFlag", "BACKORDERFLAG")
      .withColumnRenamed("launchSkuFlag", "LAUNCHSKUFLAG")
      .withColumnRenamed("taxCode", "TAXCODE")
      .withColumnRenamed("productDesignator", "PRODUCTDESIGNATOR")
      .withColumnRenamed("productNumber", "PRODUCTNUMBER")
      .withColumnRenamed("productType", "PRODUCTTYPE")
      .withColumnRenamed("cegrRefId", "CEGRREFID")
      .withColumnRenamed("originalRetailPrice", "ORIGINALRETAILPRICE")
      .withColumnRenamed("originalUnitPrice", "ORIGINALUNITPRICE")
      .withColumnRenamed("originalUnitDiscountAmount", "ORIGINALUNITDISCOUNTAMOUNT")
      .withColumnRenamed("lineRefundSubTotal", "LINEREFUNDSUBTOTAL")
      .withColumnRenamed("taxAmount", "TAXAMOUNT")
      .withColumnRenamed("shippingAmount", "SHIPPINGAMOUNT")
      .withColumnRenamed("shippingTaxAmount", "SHIPPINGTAXAMOUNT")
      .withColumnRenamed("inboundShippingAmount", "INBOUNDSHIPPINGAMOUNT")
      .withColumnRenamed("totalAmount", "TOTALAMOUNT")
      .withColumnRenamed("returnExpected", "RETURNEXPECTED")
      .withColumnRenamed("returned", "RETURNED")
      .withColumnRenamed("returnNumbers", "RETURNNUMBERS")
      .withColumnRenamed("quantityReturned", "QUANTITYRETURNED")
      .withColumnRenamed("returnDate", "RETURNDATE")
      .withColumnRenamed("reasonCode", "REASONCODE")
      .withColumnRenamed("ih_inboundShippingAmount", "IH_INBOUNDSHIPPINGAMOUNT")
      .withColumnRenamed("ih_lineRefundSubTotal", "IH_LINEREFUNDSUBTOTAL")
      .withColumnRenamed("ih_originalRetailPrice", "IH_ORIGINALRETAILPRICE")
      .withColumnRenamed("ih_originalUnitDiscountAmount", "IH_ORIGINALUNITDISCOUNTAMOUNT")
      .withColumnRenamed("ih_originalUnitPrice", "IH_ORIGINALUNITPRICE")
      .withColumnRenamed("ih_shippingAmount", "IH_SHIPPINGAMOUNT")
      .withColumnRenamed("ih_shippingTaxAmount", "IH_SHIPPINGTAXAMOUNT")
      .withColumnRenamed("ih_taxAmount", "IH_TAXAMOUNT")
      .withColumnRenamed("ih_totalAmount", "IH_TOTALAMOUNT")
      .withColumnRenamed("refundMethods", "REFUNDMETHODS")
      .withColumnRenamed("paymentsInfo", "PAYMENTSINFO")
      .withColumnRenamed("refund_notes", "REFUND_NOTES")
      .withColumnRenamed("refunds_statusHistory", "REFUNDS_STATUSHISTORY")
      .withColumnRenamed("load_time_kafka", "LOAD_TIME_KAFKA")
      .withColumnRenamed("load_time_adls", "LOAD_TIME_ADLS")
      .withColumnRenamed("order_date", "ORDER_DATE")
      .withColumnRenamed("postedAt", "POSTEDAT")
      .withColumnRenamed("postedBy", "POSTEDBY")
      .withColumnRenamed("lineNumber", "LINENUMBER")
      .withColumnRenamed("order_id", "ORDER_ID")
      .withColumnRenamed("order_datetime", "ORDER_DATETIME")
      .withColumnRenamed("refundId", "REFUNDID")
      .select(snowflakeCols.map(col): _*)
    writeSnowflakeTable(
      getSnowflakeConnectionOptions(),
      getTableName("REFUNDS", inputParams(AppendGen2ToSnowFlakeTablesFlag)),
      refunds_parsed_snow,
      SaveMode.Append)

    val partitionWindow = Window.partitionBy($"companyNumber",$"order_id",$"sku",$"size",$"refundId",$"refundNum",$"linenumber").orderBy($"postedAt".desc)
    val rownumber = row_number().over(partitionWindow)
    val refunds_refined = refunds_parsed.select($"*", rownumber as "rnum").filter($"rnum" === "1")
      .drop("rnum")
      .withColumn("updated_datetime",col("load_time_adls")).withColumn("ref_source", lit("OMS"))
    refunds_refined.createOrReplaceTempView("refunds_refined")

//    val mergequery = Source.fromInputStream(AdlsRefund_Gen2.getClass.getResourceAsStream("/sqls/mergerefunds.sql")).mkString
//    snowflakequeryrun(mergequery,Options)

    oms_refund_input.sparkSession.sql(s"""
          MERGE INTO sales_gen2.oms_refunds t
          USING refunds_refined s
          ON s.order_id = t.order_id AND s.sku = t.sku and t.companyNumber = s.companyNumber
          and s.size = t.size and s.refundId = t.refundId and s.refundNum =t.refundNum
          and s.linenumber = t.linenumber
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
          WHEN NOT MATCHED THEN INSERT (order_id, order_datetime, refundId, companyNumber, refund_header_date, refund_status, refund_createdBy, refund_userId, source, refundNum, lineNumber, quantity, name, sku, size, color, image, brand, category, description, isCollectUpFront, backorderFlag, launchSkuFlag, taxCode, productDesignator, productNumber, productType, cegrRefId, originalRetailPrice, originalUnitPrice, originalUnitDiscountAmount, lineRefundSubTotal, taxAmount, shippingAmount, shippingTaxAmount, inboundShippingAmount, totalAmount, returnExpected, returned, returnNumbers, quantityReturned, returnDate, reasonCode, ih_inboundShippingAmount, ih_lineRefundSubTotal, ih_originalRetailPrice, ih_originalUnitDiscountAmount, ih_originalUnitPrice, ih_shippingAmount, ih_shippingTaxAmount, ih_taxAmount, ih_totalAmount, refundMethods, paymentsInfo, refund_notes, refunds_statusHistory, load_time_kafka, load_time_adls, order_date, postedAt, postedBy, updated_datetime, ref_source)
          VALUES (s.order_id, s.order_datetime, s.refundId, s.companyNumber, s.refund_header_date, s.refund_status, s.refund_createdBy, s.refund_userId, s.source, s.refundNum, s.lineNumber, s.quantity, s.name, s.sku, s.size, s.color, s.image, s.brand, s.category, s.description, s.isCollectUpFront, s.backorderFlag, s.launchSkuFlag, s.taxCode, s.productDesignator, s.productNumber, s.productType, s.cegrRefId, s.originalRetailPrice, s.originalUnitPrice, s.originalUnitDiscountAmount, s.lineRefundSubTotal, s.taxAmount, s.shippingAmount, s.shippingTaxAmount, s.inboundShippingAmount, s.totalAmount, s.returnExpected, s.returned, s.returnNumbers, s.quantityReturned, s.returnDate, s.reasonCode, s.ih_inboundShippingAmount, s.ih_lineRefundSubTotal, s.ih_originalRetailPrice, s.ih_originalUnitDiscountAmount, s.ih_originalUnitPrice, s.ih_shippingAmount, s.ih_shippingTaxAmount, s.ih_taxAmount, s.ih_totalAmount, s.refundMethods, s.paymentsInfo, s.refund_notes, s.refunds_statusHistory, s.load_time_kafka, s.load_time_adls, s.order_date, s.postedAt, s.postedBy, s.updated_datetime, s.ref_source)
        """)

    //refunds_parsed.unpersist()

  }
}