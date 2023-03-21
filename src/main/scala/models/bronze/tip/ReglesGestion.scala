package models.bronze.tip

import datawrap.core.{Materialization, SparkModel, Variables}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, functions => f}
import stats.ConstantesColonnes.{COLSREGLEGESTIONFINAL, COLSREGLEGESTIONINIT}
import utils.FonctionsUtiles.unionLeftDFwithRightDF

object ReglesGestion extends SparkModel {

  override val declaredVariables: Set[String] = Set()

  override val inputs: Set[SparkModel] = Set(SviTaxJoinFloue)

  override val materialization: Materialization = Materialization.FILE("ORC")

  override protected def compute(implicit variables: Variables): DataFrame = {

    // Créer de nouveaux champs a partir de InternFacilities
    // A noter qu'il faut incrémenter par 1 la position par rapport à la règle sur KNIME
    val sviTaxationSubstr = SviTaxJoinFloue
      .withColumn("CallForwardingUnconditional", f.substring(f.col("InternFacilities"), 6, 1).cast("Integer"))
      .withColumn("CallForwardingOnBusy", f.substring(f.col("InternFacilities"), 7, 1).cast("Integer"))
      .withColumn("CallForwardingOnNoReply", f.substring(f.col("InternFacilities"), 8, 1).cast("Integer"))
      .withColumn("VoiceMail", f.substring(f.col("InternFacilities"), 32, 1).cast("Integer"))
      .withColumn("Transfer", f.substring(f.col("InternFacilities"), 9, 1))
      .withColumn("BasicCall", f.substring(f.col("InternFacilities"), 23, 1))
      .withColumn("OperatorFacility", f.substring(f.col("InternFacilities"), 24, 1))
      .withColumn("Substitution", f.substring(f.col("InternFacilities"), 24, 1))
      .withColumn("PriorityIncomingCall", f.substring(f.col("InternFacilities"), 26, 1))
      .withColumn("Transit", f.substring(f.col("InternFacilities"), 27, 1))
      .withColumn("PrivateOverflowToPublic", f.substring(f.col("InternFacilities"), 28, 1))
      .withColumn("ReroutingPublicToPrivate", f.substring(f.col("InternFacilities"), 29, 1))
      .withColumn("FaxServer", f.substring(f.col("InternFacilities"), 30, 1))
      .withColumn("CentralAbbreviatedNumberring", f.substring(f.col("InternFacilities"), 32, 1))
      .withColumn("IntegratedServiceVPN", f.substring(f.col("InternFacilities"), 34, 1))
      .withColumn("OverflowVPN", f.substring(f.col("InternFacilities"), 35, 1))
      .withColumn("ARSService", f.substring(f.col("InternFacilities"), 36, 1))
      .withColumn("Disa", f.substring(f.col("InternFacilities"), 37, 1))

    val listeColonnesRg = COLSREGLEGESTIONINIT

    val ListAddCols = List(
      "NewInitialStartDateTime",
      "NewCalledNumber",
      "NewStartDateTime",
      "not_transfered",
      "calledNumber_externe_interne",
      "Type_appel"
    )

    val ListColsAllCalls = listeColonnesRg ::: ListAddCols

    // Ne garder que les champs préciser sur KNIME
    val sviTaxationGoodColsSrc = sviTaxationSubstr
      .withColumnRenamed("SAGES (bloc)", "SAGES")
      .withColumnRenamed("bloc name", "bloc_name")
      .withColumnRenamed("bloc duration", "bloc_duration")
      .withColumnRenamed("bloc id", "bloc_id")
      .withColumnRenamed("Motif appel", "motif_appel")
      .withColumnRenamed("Type appel", "type_appel")
      .withColumnRenamed("CalledNumber Externe – Interne", "calledNumber_externe_interne")
      .selectExpr(listeColonnesRg: _*)
    // convertir dans les bons formats
    val sviTaxationGoodCols = sviTaxationGoodColsSrc
      .withColumn("EffectiveCallDuration", f.col("EffectiveCallDuration").cast("Integer"))
      .withColumn("duration", f.col("duration").cast("Double"))
      .withColumn("bloc_duration", f.col("bloc_duration").cast("Double"))

    // ***************************************************************************************************************//
    // Calcul des règles de gestion

    // Cas 1 : récuperer les calls interne ( call Type = 4 )
    val internCallsType4 =
      sviTaxationGoodCols.filter(sviTaxationGoodCols("endcause").isNotNull && sviTaxationGoodCols("CallType") === 4)

    val internCallsType4Grouped =
      internCallsType4
        .groupBy("InitialStartDateTime", "CalledNumber")
        .agg(f.min("StartDateTime").as("NewStartDateTime"))

    val internCallsType4RenamedCols = internCallsType4Grouped
      .withColumn("not_transfered", f.lit(0))
      .withColumnRenamed("InitialStartDateTime", "NewInitialStartDateTime")
      .withColumnRenamed("CalledNumber", "NewCalledNumber")

    // Première Jointure "Node 461", entre la jointure floue globale et les calls internes
    val callsInternWithSviTaxationJoined = sviTaxationGoodCols
      .join(
        internCallsType4RenamedCols,
        f.col("InitialStartDateTime") === f.col("NewInitialStartDateTime") && f.col("CalledNumber") === f.col(
          "NewCalledNumber"
        ) && f.col("StartDateTime") === f.col("NewStartDateTime"),
        "leftouter"
      )
      .withColumn(
        "not_transfered",
        f.when(f.col("not_transfered").isNull, 1)
          .when(f.col("not_transfered") === 0, 0)
          .otherwise(null)
      )
      .withColumn("Transfer", f.col("Transfer") * f.col("not_transfered"))
      .where(f.col("ChargedNumber").isNotNull)

    /** *********************************** cas 1 CallType != 14 *****************************************************
      */
    var callsNot14 = callsInternWithSviTaxationJoined.where("CallType != 14")
    // création de nouveaux champs
    callsNot14 = callsNot14
      .withColumn(
        "Type_appel",
        f.when(
          f.col("CallType") === 0 || f.col("CallType") === 3 || f.col("CallType") === 7 || f.col("CallType") === 8,
          "Appel émis"
        ).when(f.col("CallType") === 4 || f.col("CallType") === 9 || f.col("CallType") === 13, "Appel reçu")
          .when(f.col("CallType") === 14, "Appel interne local")
          .otherwise("Inconnu")
      )
      .withColumn(
        "calledNumber_externe_interne",
        f.when(f.length(f.col("CalledNumber")) > 8 || f.length(f.col("CalledNumber")) === 4, "Externe")
          .when(f.length(f.col("CalledNumber")) <= 8, "Interne")
          .otherwise("Inconnu")
      )
      .withColumn(
        "ChargedNumber",
        f.when(f.col("Transfer") === 1, f.col("CallingNumber"))
          .otherwise(f.col("ChargedNumber"))
      )
      .selectExpr(ListColsAllCalls: _*)

    /** ***************************************** cas 2 CallType == 14
      * *****************************************************
      */
    // Pour les calls internes , chaque appel est compté comme appel émis et reçus à la fois
    // L'idée donc est de dupliquer les lignes des appels internes et identifier chaque ligne par le type appel : émis ou reçus

    var calls14 = callsInternWithSviTaxationJoined.where("CallType == 14")

    calls14 = calls14.withColumn("calledNumber_externe_interne", f.lit("Interne"))

    // Première partie  Appel émis
    val calls14Emis = calls14
      .withColumn("Type_appel", f.lit("Appel émis"))
      .selectExpr(ListColsAllCalls: _*)

    // Deuxième partie  Appel reçu
    val calls14Recu = calls14
      .withColumn("Type_appel", f.lit("Appel reçu"))
      .withColumn("temp_CalledNumber", f.col("ChargedNumber"))
      .withColumn("ChargedNumber", f.col("CalledNumber"))
      .drop("CalledNumber", "endcause")
      .withColumnRenamed("temp_CalledNumber", "CalledNumber")
      .withColumn("endcause", f.lit(null).cast(StringType))
      . // ajout à nouveau de endcause pour ne pas avoir la colonne manquante pour la jointure (purement spark)
      selectExpr(ListColsAllCalls: _*)

    // Remplacer les calls internes par l'union des calls internes émis et reçus
    calls14 = unionLeftDFwithRightDF(calls14Emis, calls14Recu)

    /** ********************************* cas 3 appel transférées Call Type = 9
      * ***********************************************
      */
    val calls9Src = callsInternWithSviTaxationJoined.where((f.col("Transfer") === 1) and (f.col("CallType") =!= 9))
    val calls9Selected = calls9Src
      .withColumn("calledNumber_externe_interne", f.lit("Interne"))
      .drop("Transfer")
      .withColumn("Transfer", f.lit(null).cast(StringType))

    // Première partie : identifier les appels émis
    val calls9Emis = calls9Selected
      .withColumn("Type_appel", f.lit("Appel émis"))
      .withColumn("CalledNumber", f.col("ChargedNumber"))
      .withColumn("ChargedNumber", f.col("CallingNumber"))
      .selectExpr(ListColsAllCalls: _*)

    // Deuxième partie : identifier les appels reçu
    val calls9Recu = calls9Selected
      .withColumn("Type_appel", f.lit("Appel reçu"))
      .withColumn("CalledNumber", f.col("CallingNumber"))
      .selectExpr(ListColsAllCalls: _*)

    // Union des calls reçus et émis et récupération des appels transférés
    var calls9 = unionLeftDFwithRightDF(calls9Emis, calls9Recu)

    // Supprimer( vider ) end cause pour le récupérer après la jointure
    calls9 = calls9
      .drop("endcause")
      .withColumn("endcause", f.lit(null).cast(StringType))

    /** **************************************** Union des 3 cas
      * ******************************************************************
      */

    // Union final entre les 3 types de calls
    val unionCalls14AndNot14 = unionLeftDFwithRightDF(callsNot14, calls14)
    var unionCalls9to14      = unionLeftDFwithRightDF(unionCalls14AndNot14, calls9)

    /** **************************************** Indicateurs
      * ******************************************************************
      */
    // Création Des champs contenant les nouvelles règles de gestion
    unionCalls9to14 = unionCalls9to14
      .withColumn(
        "Origine",
        f.when(f.col("endcause").isNotNull, "SVI")
          .when(f.col("CallType") === 9 && f.col("endcause").isNull, "SVI non reconnu")
          .otherwise("Direct")
      )
      // a modifier ---------------------------------------
      .withColumn(
        "Origine_appel_entrant",
        f.when(f.col("Type_appel") === "Appel reçu" && f.col("Origine") === "SVI", "SVI")
          .when(
            f.col("Type_appel") === "Appel reçu" && f.col("Origine") === "Direct" && f.col(
              "calledNumber_externe_interne"
            ) === "Externe",
            "Appel entrant direct externe"
          )
          .when(
            f.col("Type_appel") === "Appel reçu" && f.col("Origine") === "Direct" && f.col(
              "calledNumber_externe_interne"
            ) === "Interne",
            "Appel entrant direct interne"
          )
          .otherwise(null)
      )
      .withColumnRenamed("Transfer", "Appel_transfere")
      .withColumn(
        "postes_logges",
        f.when(f.col("CallType") === 4, f.col("chargedNumber"))
          .otherwise(null)
      )
      .withColumn("Date", f.col("InitialStartDateTime").substr(0, 10))
      .withColumn(
        "Appels_recus",
        f.when(f.col("CallType") === 9 && f.col("endcause") === "TRANSFERRED", 0)
          .when(f.col("Appel_Transfere") === 1, 0)
          .when(f.col("Type_appel") === "Appel reçu", 1)
          .otherwise(null)
      )
      .withColumn(
        "Appels_emis",
        f.when(f.col("Type_appel") === "Appel émis", 1)
          .otherwise(0)
      )

    val appelsCount = unionCalls9to14.groupBy("CalledNumber", "Date").agg(f.sum("Appels_recus").as("sum_appels_recus"))
    // unionCalls9to14.printSchema()

    val reglesFinal = unionCalls9to14
      .join(appelsCount, Seq("CalledNumber", "Date"), "leftouter")
      .withColumn("Appels", f.when(f.col("Type_appel") === "Appel reçu", f.col("sum_appels_recus")))
      .withColumn("bloc_duration", f.col("bloc_duration").cast("double"))
      .withColumn(
        "Mise_en_relation",
        f.when(f.col("endcause") === "TRANSFERRED", f.col("Appels_recus"))
          .when(
            f.col("endcause") === "RELEASED By Caller" && f.col("Motif_appel").isNotNull && f
              .col("bloc_duration")
              .cast(
                "float"
              ) > 60,
            f.col("Appels_recus")
          )
          .when(f.col("endcause") === "RELEASED By VAA" && f.col("Motif_appel").isNotNull, f.col("Appels_recus"))
      )
      .withColumn(
        "Appels_SVI",
        f.when(f.col("Origine") === "SVI" && !(f.col("endcause") <=> "NOT ENOUGH LICENSE"), f.col("Appels_recus"))
      )
      .withColumn(
        "Appels_dissuades",
        f.when(f.col("endcause") === "RELEASED By VAA" && f.col("Motif_appel").isNull, f.col("Appels_recus"))
      )
      .withColumn(
        "Appels_raccroches",
        f.when(
          f.col("endcause") === "RELEASED By Caller" && (f.col("Motif_appel").isNull || (f
            .col(
              "Motif_appel"
            )
            .isNotNull && f.col("bloc_duration").cast("double") <= 60)),
          f.col("Appels_recus")
        )
      )
      .withColumn("Appels_sans_license", f.when(f.col("endcause") === "NOT ENOUGH LICENSE", f.col("Appels_recus")))
      .withColumn(
        "Appels_SVI_non_reconnu",
        f.when(f.col("CallType") === 9 && f.col("endcause").isNull, f.col("Appels_recus"))
      )
      .withColumn(
        "Appels_directs_internes",
        f.when(
          f.col("Origine") === "Direct" && f.col("calledNumber_externe_interne") === "Interne",
          f.col("Appels_recus")
        )
      )
      .withColumn(
        "Appels_directs_externes",
        f.when(
          f.col("Origine") === "Direct" && f.col("calledNumber_externe_interne") === "Externe",
          f.col("Appels_recus")
        )
      )
      .withColumn(
        "Appels_aboutis",
        f.when(
          f.col("Appels_directs_internes") === 1 || f.col("Appels_directs_externes") === 1 || f.col(
            "Mise_en_relation"
          ) === 1,
          f.col("Appels_recus")
        )
      )
      // Il existe un problème sur ce champs, le nombre des calls décrochés ( somme des 1 ) est supérieur de 600 calls
      .withColumn(
        "Appels_decroches",
        f.when(
          // expr("not (endcause = 'RELEASED By Caller') and Appels_aboutis = 1 and EffectiveCallDuration > 0 ")
          // f.col("Appels_aboutis") === "1" && f.col("EffectiveCallDuration") > 0 &&  !(f.col("endcause")isin("RELEASED By Caller" ))
          f.col("Appels_aboutis") === "1" && f.col("EffectiveCallDuration") > 0 && !(f.col(
            "endcause"
          ) === "RELEASED By Caller") && !(f.col(
            "endcause"
          ) === "RELEASED By VAA"),
          f.col("Appels_recus")
        ).otherwise(0)
      )
      .withColumn(
        "Appels_non_decroches",
        f.when(
          (f.col("Appels_aboutis") === 1 && f.col("EffectiveCallDuration") === 0) || (f.col(
            "endcause"
          ) === "RELEASED By Caller" && f.col("Motif_appel").isNotNull && f.col("bloc_duration") > 60) || (f.col(
            "endcause"
          ) === "RELEASED By VAA" && f.col("Motif_appel").isNotNull),
          f.col("Appels_recus")
        )
      )
      .withColumn(
        "Appels_transferes",
        f.when(f.col("Appel_transfere") === "1.0", f.col("Appel_transfere").cast("Integer"))
      )
      .withColumn(
        "Appels_renvoyes",
        f.when(
          f.col("CallForwardingUnconditional") === 1 || f.col("CallForwardingOnBusy") === 1 || f.col(
            "CallForwardingOnNoReply"
          ) === 1,
          f.col("Appels_recus")
        )
      )
      .withColumn(
        "Attente_avant_mise_en_relation",
        f.when(f.col("endcause") === "TRANSFERRED" && f.col("EffectiveCallDuration") > 0, f.col("bloc_duration"))
          .when(f.col("Type_appel") === "Appel reçu" && f.col("Origine") === "Direct", f.col("WaitingDuration"))
          .when(f.col("CallType") === 9 && f.col("endcause") === "TRANSFERRED", 0)
      )
      .withColumn("Duree_de_navigation", f.when(f.col("CallType") === 9, f.col("EffectiveCallDuration")))
      .withColumn(
        "Duree_de_communication",
        f.when(f.col("CallType") === 9, 0)
          .when(f.col("Type_appel") === "Appel reçu", f.col("EffectiveCallDuration"))
          .when(f.col("Type_appel") === "Appel émis", f.col("EffectiveCallDuration"))
      )
      .withColumn(
        "Aboutissement_SVI",
        f.when(f.col("Appels_non_decroches") === 1, "Appel non décroché")
          .when(f.col("Appels_dissuades") === 1, "Appel dissuadé")
          .when(f.col("Appels_raccroches") === 1, "Appel raccroché")
          .when(f.col("Appels_decroches") === 1, "Appel décroché")
          .otherwise(null)
      )
      .withColumn(
        "Horaire_ouverture",
        f.when((f.col("Motif_appel").isNotNull) || (f.col("Origine") === "Direct"), f.lit("Horaire d’ouverture"))
          .when(f.col("Motif_appel").isNull && f.col("Origine") === "SVI", f.lit("Horaire de fermeture"))
      )

    // reglesFinal.show(false)
    // sys.exit(0)

    // ayoub : j'ai rajouté "Horaire_ouverture" dans la liste COLSREGLEGESTIONFINAL
    val reglesDeGestion = reglesFinal.selectExpr(COLSREGLEGESTIONFINAL: _*)
    reglesDeGestion

  }

}
