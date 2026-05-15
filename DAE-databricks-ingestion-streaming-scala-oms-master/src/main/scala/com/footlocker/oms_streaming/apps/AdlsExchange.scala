package com.footlocker.oms_streaming.apps

import com.footlocker.conf.AppConfObject.Options
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import com.footlocker.services.SnowflakeHelperService
import com.footlocker.utils.Common.{applyschema}
import com.footlocker.services.DeltaoptimizeService


object AdlsExchange  extends SnowflakeHelperService {

  def exchange_adls_push(oms_exchange: DataFrame,spark:SparkSession,inputParams : Map[String, String]): Unit = {
    oms_exchange.sparkSession.conf.set("spark.sql.shuffle.partitions", 1)
    oms_exchange.sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    oms_exchange.sparkSession.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
    import spark.implicits._

    //    val oms_exchange_df = oms_exchange.withColumn("EXCHANGE_flag", when($"json".contains("""messageType":"EXCHANGE"""), "1").when($"json".contains("""messageType" : "EXCHANGE"""), "1").otherwise("0"))
    //      .filter("EXCHANGE_flag == 1")
    //      .drop("EXCHANGE_flag")
    val schemapath:String = (inputParams("schema_location")+"/exchanges.json")
    val parsed = applyschema(spark,oms_exchange,schemapath)

    val oms_exchange_latest = parsed.select("oms_parsed_data.orderId",
      "oms_parsed_data.exchangeId",
      "oms_parsed_data.order",
      "oms_parsed_data.postedAt",
      "oms_parsed_data.postedBy",
      "oms_parsed_data.exchangeStatus",
      "load_time_kafka",
      "load_time_adls"
    )

    val exchange_explode_1 = oms_exchange_latest.select(
      trim(oms_exchange_latest("orderId")) as "order_id",
      trim(oms_exchange_latest("exchangeId")) as "exchangeId",
      oms_exchange_latest("order") as "order",
      explode_outer(oms_exchange_latest("order.exchanges")).alias("exchanges"),
      trim(oms_exchange_latest("postedAt")) as "postedAt",
      $"postedBy",
      $"exchangeStatus",
      $"load_time_kafka",
      $"load_time_adls"
    )

    val exchange_explode_2 = exchange_explode_1.select(
      $"order_id",
      $"exchangeId",
      $"order",
      $"exchanges",
      explode_outer($"exchanges.fullfillmentGrouping") as "fullfillmentGrouping",
      $"postedAt",
      $"postedBy",
      $"exchangeStatus",
      $"load_time_kafka",
      $"load_time_adls"
    )

    val exchange_explode_3 = exchange_explode_2.select(
      $"order_id",
      $"exchangeId",
      $"order",
      $"exchanges",
      $"fullfillmentGrouping",
      explode_outer($"fullfillmentGrouping.orderLines") as "lines",
      $"postedAt",
      $"postedBy",
      $"exchangeStatus",
      $"load_time_kafka",
      $"load_time_adls"
    )


    val exchange_parsed = exchange_explode_3.select(
      $"order_id",
      $"exchangeId",
      $"exchangeStatus" as "Header_exchangeStatus",
      trim($"order.orderHeader.companyNumber") as "companyNumber",
      $"exchanges.createdBy" as "exchange_createdBy",
      $"exchanges.userId" as "userId",
      $"exchanges.source" as "source",
      $"exchanges.date" as "exchange_header_date",
      $"exchanges.exchangeNum" as "exchangeNum",
      $"exchanges.returnNum" as "returnNum",
      $"exchanges.exchangeOrderNum" as "exchangeOrderNum",
      $"exchanges.status" as "exchange_status",
      $"fullfillmentGrouping.fullfillmentType" as "fullfillmentType",
      $"fullfillmentGrouping.shipMethod" as "shipMethod",
      $"fullfillmentGrouping.shipMethodDesc" as "shipMethodDesc",
      $"fullfillmentGrouping.shippingAmount.currencyIso" as "shippingAmount_currencyIso",
      $"fullfillmentGrouping.shippingAmount.value" as "shippingAmount_value",
      $"fullfillmentGrouping.shippingLine" as "shippingLine",
      $"lines.lineNumber" as "lineNumber",
      $"lines.saleCode" as "line_saleCode",
      $"lines.taxCode" as "line_taxCode",
      $"lines.giftMessage" as "line_",
      $"lines.giftReceipientEmail" as "line_giftReceipientEmail",
      $"lines.giftFrom" as "line_giftFrom",
      $"lines.giftTo" as "line_giftTo",
      $"lines.giftCardNum" as "line_giftCardNum",
      $"lines.shipMethod" as "line_shipMethod",
      $"lines.freeShipping" as "line_freeShipping",
      $"lines.quantity" as "line_quantity",
      $"lines.inventoryLocation" as "line_inventoryLocation",
      $"lines.product.name" as "name",
      $"lines.product.image" as "image",
      $"lines.product.sku" as "sku",
      $"lines.product.size" as "size",
      $"lines.product.color" as "color",
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
      $"lines.orderLinePricing.currencyIso" as "currencyIso",
      $"lines.orderLinePricing.priceOverrideReason" as "priceOverrideReason",
      $"lines.orderLinePricing.originalRetailPrice" as "originalRetailPrice",
      $"lines.orderLinePricing.UnitPrice" as "UnitPrice",
      $"lines.orderLinePricing.subTotalAmount" as "subTotalAmount",
      $"lines.orderLinePricing.taxAmount" as "taxAmount",
      $"lines.orderLinePricing.shippingAmount" as "shippingAmount",
      $"lines.orderLinePricing.shippingTaxAmount" as "shippingTaxAmount",
      $"lines.orderLinePricing.giftBoxAmount" as "giftBoxAmount",
      $"lines.orderLinePricing.giftBoxTaxAmount" as "giftBoxTaxAmount",
      $"lines.orderLinePricing.totalAmount" as "totalAmount",
      $"lines.orderLinePricing.discountAmount" as "discountAmount",
      $"lines.orderLinePricing.discountedTotalAmount" as "discountedTotalAmount",
      $"exchanges.paymentRequest" as "paymentRequest",
      $"exchanges.paymentsInfo" as "paymentsInfo",
      $"exchanges.alternateShippingAddress.firstName" as "alternateShipping_firstName",
      $"exchanges.alternateShippingAddress.lastName" as "alternateShipping_lastName",
      $"exchanges.alternateShippingAddress.email" as "alternateShipping_email",
      $"exchanges.alternateShippingAddress.companyName" as "alternateShipping_companyName",
      $"exchanges.alternateShippingAddress.phoneNumber" as "alternateShipping_phoneNumber",
      $"exchanges.alternateShippingAddress.addressLine1" as "alternateShipping_addressLine1",
      $"exchanges.alternateShippingAddress.addressLine2" as "alternateShipping_addressLine2",
      $"exchanges.alternateShippingAddress.city" as "alternateShipping_city",
      $"exchanges.alternateShippingAddress.state" as "alternateShipping_state",
      $"exchanges.alternateShippingAddress.stateCode" as "alternateShipping_stateCode",
      $"exchanges.alternateShippingAddress.country" as "alternateShipping_country",
      $"exchanges.alternateShippingAddress.countryCode" as "alternateShipping_countryCode",
      $"exchanges.alternateShippingAddress.postalCode" as "alternateShipping_postalCode",
      $"order.orderHeader.orderDateTime" as "orderDateTime",
      $"postedAt",
      $"postedBy",
      $"load_time_kafka",
      $"load_time_adls"
    ).withColumn("orderdate", to_date($"orderDateTime"))

    //exchange_parsed.persist()

    writeSnowflakeTable(Options,
      "EXCHANGES",
      exchange_parsed,
      SaveMode.Append)

    exchange_parsed.write.format("delta").mode("append").partitionBy("orderdate").save(inputParams("adls_url")+"/landing/sales/oms/exchanges")

    //exchange_parsed.unpersist()
  }





}