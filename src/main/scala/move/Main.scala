package move

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf


object Main {

  def main(args: Array[String]): Unit = {

    //MI AGGANCIO AL DB MULTI DI MOVE-IN E ALL'AREA DI STAGING

    val conf = new SparkConf().setAppName("MoveIn")
    val spark = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

    import spark.implicits._

    //import sys.process._

    //println("hdfs dfs -ls /user/fcarapelle".!!)

    //"hdfs dfs -rm -r /user/fcarapelle/join_df_02".!!

    //MI AGGANCIO AL DB MULTI DI MOVE-IN E ALL'AREA DI STAGING

    val db_multi = "db_regpie_multi_movein"
    val stg = "stg_regpie_regpie_movein"

    val hive = com.hortonworks.hwc.HiveWarehouseSession.session(spark).build()

    import java.time.format.DateTimeFormatter
    import java.time.LocalDateTime

    val data_oggi = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(LocalDateTime.now)

    //******************************************************************************************INIZIO_DEGLI_STEP_PREP*************************************************************************************************

    //PULIZIA DATAFRAME

    var df_storico = hive.executeQuery(s"""
      select *
      from $db_multi.movein_piemonte
      where tipo_adesione is not null and
      data_adesione is not null and
      data_attivazione is not null and
      data_fine_adesione is not null
      """).
      withColumn("data_adesione", to_date($"data_adesione", "dd-MM-yyyy")).
      withColumn("data_attivazione", to_date($"data_attivazione", "dd-MM-yyyy")).
      withColumn("data_fine_adesione", to_date($"data_fine_adesione", "dd-MM-yyyy")).
      withColumn("data_revoca_movein", to_date($"data_revoca_movein", "dd-MM-yyyy")).
      withColumn("data_riattivazione", to_date($"data_riattivazione", "dd-MM-yyyy")).
      withColumn("data_oggi", to_date(lit(s"$data_oggi"), "yyyy-MM-dd")).
      withColumn("verifica", when(datediff(col("data_fine_adesione"), col("data_attivazione")) === 365 or
        datediff(col("data_fine_adesione"), col("data_attivazione")) === 366, lit("true")).otherwise(lit("false"))). //366 per gli anni bisestili
      withColumn("data_revoca_movein", when($"data_revoca_movein" === $"data_attivazione", to_date(lit("0000-00-00"), "yyyy-MM-dd")).otherwise($"data_revoca_movein")).

      withColumn("data_revoca_movein", when($"data_revoca_movein".isNotNull and $"data_riattivazione".isNotNull and $"data_riattivazione" >= $"data_revoca_movein"
        and datediff($"data_riattivazione", $"data_revoca_movein") <= 90, null).otherwise($"data_revoca_movein")). //CASO IN CUI VI SIANO ANOMALIE SULLE DATE REVOCA INCREMENTALI ED ESISTA UNA DATA RIATTIVAZIONE CONSIDERIAMO DA SANARE TUTTE QUELLE DATE REVOCA CHE HANNO COME DELTA DATA RIATTIVAZIONE - DATA REVOCA <= 90 (GIORNI) CONSIDERANDOLE COME ANOMALIE METTIAMO LA DATA REVOCA A NULL

      where("data_adesione <= data_attivazione and verifica = 'true' and ( data_revoca_movein != data_attivazione or data_revoca_movein is null ) ").

      orderBy(desc("targa_hmac"))

    //to_date(lit("0000-00-00"), "yyyy-MM-dd" SERVE SOLO PER METTERE A NULL LE DATE REVOCA CHE PRESENTANO QUELL'ANOMALIA, VA BENE ANCHE AL POSTO DI 0000 ECC, METTERE null

    df_storico.write.mode("overwrite").format("Parquet").save("df_storico")

    df_storico = spark.read.load("df_storico")

    //******************************************************************************************************END_1*************************************************************************************************************

    //DATA QUALITY SUL DF, EREDITATI DA LAURA

    val colNames = Array("a1_u_eco", "a1_a_noeco", "a1_u_noeco", "a1_a_eco", "a1_e", "a2_e", "a2_u_eco", "a2_u_noeco", "a2_a_noeco", "a2_a_eco")

    for (colName <- colNames) {
      df_storico = df_storico.withColumn(colName, translate(col(colName), ",", ".").cast("Double")).
        na.fill(0, Seq(colName)).withColumn(colName, round(col(colName), 2))
    }

    //RINOMINA COLONNE IN MODO PIU ESPLICITO

    val df_storico_01 = df_storico.withColumnRenamed("a1_u_eco", "Km_a1_u_eco").
      withColumnRenamed("a1_u_noeco", "Km_a1_u_noeco").
      withColumnRenamed("a1_a_eco", "Km_a1_a_eco").
      withColumnRenamed("a1_a_noeco", "Km_a1_a_noeco").
      withColumnRenamed("a2_u_eco", "Km_a2_u_eco").
      withColumnRenamed("a2_u_noeco", "Km_a2_u_noeco").
      withColumnRenamed("a2_a_eco", "Km_a2_a_eco").
      withColumnRenamed("a2_a_noeco", "Km_a2_a_noeco").
      withColumnRenamed("a1_e", "Km_a1_e").
      withColumnRenamed("a2_e", "Km_a2_e")

    df_storico_01.write.mode("overwrite").format("Parquet").save("df_storico1")

    val df_storico_01_1 = spark.read.load("df_storico1")

    //KM_PERCORSI TOTALI

    val km_percorsi_per_targa = df_storico_01_1.
      withColumn("Km_tot_eco", round($"Km_a1_u_eco" + $"Km_a1_a_eco" + $"Km_a2_u_eco" + $"Km_a2_a_eco", 2)).
      withColumn("Km_tot_noeco", round($"Km_a1_u_noeco" + $"Km_a1_a_noeco" + $"Km_a2_u_noeco" + $"Km_a2_a_noeco", 2)).
      withColumn("Km_tot_a1", round($"Km_a1_u_eco" + $"Km_a1_a_noeco" + $"Km_a1_u_noeco" + $"Km_a1_a_eco" + $"Km_a1_e", 2)).
      withColumn("Km_tot_a2", round($"Km_a2_e" + $"Km_a2_u_eco" + $"Km_a2_u_noeco" + $"Km_a2_a_noeco" + $"Km_a2_a_eco", 2)).
      withColumn("Km_tot_u", round($"Km_a1_u_eco" + $"Km_a1_u_noeco" + $"Km_a2_u_eco" + $"Km_a2_u_noeco", 2)).
      withColumn("Km_tot_a", round($"Km_a1_a_noeco" + $"Km_a1_a_eco" + $"Km_a2_a_noeco" + $"Km_a2_a_eco", 2)).
      withColumn("Km_tot_e", round($"Km_a1_e" + $"Km_a2_e", 2)).
      withColumn("Km_tot_a1_u", round($"Km_a1_u_eco" + $"Km_a1_u_noeco", 2)).
      withColumn("Km_tot_a1_a", round($"Km_a1_a_eco" + $"Km_a1_a_noeco", 2)).
      withColumn("Km_tot_a2_u", round($"Km_a2_u_eco" + $"Km_a2_u_noeco", 2)).
      withColumn("Km_tot_a2_a", round($"Km_a2_a_eco" + $"Km_a2_a_noeco", 2)).
      withColumn("Km_tot", round($"Km_a1_u_eco" + $"Km_a1_a_noeco" + $"Km_a1_u_noeco" + $"Km_a1_a_eco" + $"Km_a1_e" + $"Km_a2_e" + $"Km_a2_u_eco" + $"Km_a2_u_noeco" + $"Km_a2_a_noeco" + $"Km_a2_a_eco", 2)).na.fill(0)

    km_percorsi_per_targa.write.mode("overwrite").format("Parquet").save("km_percorsi_per_targa")


    val km_percorsi_per_targa_1 = spark.read.load("km_percorsi_per_targa")


    val windowSpec1 = Window.partitionBy("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale").
      orderBy("di_datafile", "categoria_veicolo", "alimentazione_veicolo", "classe_ambientale")


    val windowSpec2 = Window.partitionBy("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp").
      orderBy(asc("data_revoca_movein"))


    val WindowSpec3 = Window.partitionBy("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp").
      orderBy(asc("targa_hmac"), asc("data_attivazione"), asc("data_revoca_movein"))


    //STEP PER KM CUMULATI PER TARGA (IMPLEMENTATO CON CARLO)

    val veicoli_attivi_per_targa = km_percorsi_per_targa_1.

      withColumn("km_tot_1", lag("Km_tot", 1, 0).over(windowSpec1)).

      withColumn("diff_km_negativi", $"Km_tot" - $"Km_tot_1").

      withColumn("diff_km_prev_day",
        when($"diff_km_negativi" > 0, $"Km_tot" - $"km_tot_1").
          when($"diff_km_negativi" <= 0 && $"diff_km_negativi" > -100, lit(0).cast("Double")).
          when($"diff_km_negativi" <= 0 && $"diff_km_negativi" < -100, $"Km_tot"). //CASO IN CUI VI SIANO DEI RINNOVI O DEI FILE MANCANTI, PRENDIAMO COME DIFF_KM_PREV_DAY I KM_TOT DEL GIORNO
          otherwise(lit(0).cast("Double"))

      ).drop("km_tot_1")

    //CREA I KM CUMULATI PER OGNI TARGA E LA COLONNA NUMERO REVOCHE

    val df_storico_02 = veicoli_attivi_per_targa.
      withColumn("km_tot_cumulati", round((sum($"diff_km_prev_day").over(windowSpec1)), 2)).orderBy(asc("data_revoca_movein")) //ORDINAMENTO FONDAMENTALE PER UN CORRETTO RISULTATO

    df_storico_02.write.mode("overwrite").format("Parquet").save("df_storico_02")

    val df_storico_02_1 = spark.read.load("df_storico_02")

    //  APPLICHIAMO QUESTA CONDIZIONE PER EVITARE DEI FALSI POSITIVI SULLE DATA_REVOCA_MOVEIN, IMPLEMENTIAMO UNA LOGICA DI CONTROLLO SUI KM_TOT => SE QUELLI DI OGGI SONO MAGGIORI DI QUELLI DI IERI ALLORA L'UTENTE � UN FALSO POSITIVO (VUL DIRE CHE CI SONO ARRIVATE DELLE DATE REVOCHE ANOMALE DOVUTE A DEGLI ERRORI DELLA SCATOLETTA DEL TSP CHE NON DOBBIAMO CONSIDERARE) POICH� CAMMINA REGOLARMENTE SETTIAMO A NULL QUELLE DATA REVOCA CHE PRESENTANO QUESTA CONDIZIONE

    val df_storico_02_2 = df_storico_02_1.

      withColumn("numero_revoche", when($"data_revoca_movein".isNotNull, 1).otherwise(0)).
      withColumn("data_revoca_movein_1", lag("data_revoca_movein", 1, null).over(windowSpec2)).
      withColumn("km_tot_cumulati_1", lag("km_tot_cumulati", 1, null).over(windowSpec2)).
      withColumn("data_revoca_movein",

        when($"data_revoca_movein".isNotNull and datediff($"data_revoca_movein", $"data_revoca_movein_1") <= 90, null). //FORBICE TEMPORALE IN CUI POSSIAMO GI� TROVARE ED ESCLUDERE VARIE DATE REVOCA ANOMALE
          when($"data_revoca_movein".isNotNull and ($"km_tot_cumulati" > $"km_tot_cumulati_1"), null). //CONTROLLO PER EVITARE FALSI POSITIVI

          otherwise($"data_revoca_movein")).


      orderBy(asc("data_revoca_movein")).
      drop("data_revoca_movein_1", "km_tot_cumulati_1")

    df_storico_02_2.write.mode("overwrite").format("Parquet").save("df_storico_02_1")

    val df_storico_02_3 = spark.read.load("df_storico_02_1").orderBy(asc("km_tot_cumulati")) //ORDINAMENTO FONDAMENTALE PER UN CORRETTO RISULTATO


    val df_storico_pulito_02 = df_storico_02_3.withColumn("data_adesione_1", min($"data_adesione").over(windowSpec2)).
      drop("data_adesione").
      withColumnRenamed("data_adesione_1", "data_adesione")

    df_storico_pulito_02.write.mode("overwrite").format("Parquet").save("df_storico_pulito_02")

    val df_storico_pulito_02_1 = spark.read.load("df_storico_pulito_02").orderBy(asc("targa_hmac"), asc("data_attivazione"), asc("data_revoca_movein"))


    val df_storico_pulito_03 = df_storico_pulito_02_1.withColumn("data_attivazione_1", lead("data_attivazione", 1, null).over(WindowSpec3)).

      withColumn("data_revoca_movein", when($"data_revoca_movein" === $"data_attivazione_1", null).otherwise($"data_revoca_movein"))

    df_storico_pulito_03.write.mode("overwrite").format("Parquet").save("df_storico_pulito_03")

    val df_storico_pulito_04 = spark.read.load("df_storico_pulito_03")

    //******************************************************************************************************END_2*************************************************************************************************************

    //CREO UN NUOVO DF CHE MI SERVE PER CONTARE QUANTE DATE DI REVOCA SONO VALORIZZATE

    val df_storico_03 = df_storico_pulito_04.select("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp", "data_revoca_movein", "numero_revoche").
      where("data_revoca_movein is not null").distinct()

    //QUI AGGREGO E SOMMO SU NUMERO REVOCHE CREANDO LA NUOVA COLONNA TOTALE REVOCHE

    val df_storico_03_1 = df_storico_03.groupBy("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp").agg(sum($"numero_revoche").as("totale_revoche"))

    //EFFETTUO IL JOIN TRA IL DF STORICO PULITO E IL DF CON I TUTALI SULLE REVOCHE

    val join_df = df_storico_pulito_04.join(df_storico_03_1, Seq("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp"), "left").
      withColumn("totale_revoche", when($"totale_revoche".isNull, 0).otherwise($"totale_revoche")).drop("numero_revoche")

    //******************************************************************************************************END_3*************************************************************************************************************

    //CREO UN NUOVO DF CONTENENTE SOLO GLI UTENTI CHE ATTUALMENTE HANNO DATA REVOCA A NULL

    val df_storico_04 = df_storico_pulito_04.select("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp", "data_adesione", "data_attivazione", "data_fine_adesione", "data_revoca_movein").where("data_revoca_movein is null").distinct()


    //AGGREGAZIONE PER CONTARE IL NUMERO DI RINNOVI PER OGNI UTENTE

    val df_storico_04_1 = df_storico_04.groupBy("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp", "data_adesione", "data_attivazione").agg(count("targa_hmac").as("numero_rinnovi")).orderBy(desc("numero_rinnovi"))


    val df_storico_04_2 = df_storico_04_1.groupBy("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp").agg(sum($"numero_rinnovi").as("totale_rinnovi"))



    //JOINO I DUE DF PER OTTENERE I DETTAGLI DEI RINNOVI E REVOCHE DI OGNI UTENTE, METTO DATA REVOCA DA NULL A UN VALORE DI DEFAULT COS� DA POTER FARE IL JOIN CHE SEGUE GI� NELLA SEZIONE (V4) SENZA ERRORI


    val join_df_02 = join_df.join(df_storico_04_2, Seq("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp"), "left").
      withColumn("totale_rinnovi", when($"totale_rinnovi".isNull, 0).otherwise($"totale_rinnovi".cast("Int"))).
      withColumn("totale_revoche", when($"totale_revoche".isNull, 0).otherwise($"totale_revoche")).
      withColumn("data_revoca_movein", when($"data_revoca_movein".isNull, to_date(lit("1979-01-01"), "yyyy-MM-dd")).otherwise($"data_revoca_movein")).
      drop("numero_rinnovi")


    //******************************************************************************************************END_4*************************************************************************************************************

    //I GIORNI_DIFF MAX RISCONTRATI NEL DB SONO 379 UN TEMPO DI ERRORE ACCETTABILE CHE POSSIAMO APPROSSIMARE COME 365 O 366 SE LE CONDIZIONI SOTTO RIPORTATE VANNO A BUON FINE

    val df_finale_storico = df_storico_pulito_04.

      withColumn("data_attivazione_1", lead("data_attivazione", 1, null).over(windowSpec2)).

      withColumn("giorni_diff",

        when($"data_revoca_movein".isNull and $"data_attivazione_1" > $"data_fine_adesione", datediff(col("data_fine_adesione"), col("data_attivazione"))).

          // CON >= CONSIDERIAMO TUTTI QUEI CASI IN CUI ARRIVANO IN RITARDO LE DATE

          when($"data_revoca_movein".isNotNull and datediff(col("data_revoca_movein"), col("data_attivazione")) >= 365 and
            datediff(col("data_revoca_movein"), col("data_attivazione")) <= 379 and //RITARDO MASSIMO IN GIORNI RISCONTRATO AD OGGI
            datediff(col("data_fine_adesione"), col("data_attivazione")) === 365, 365). //DIFFERENZA CON ANNI NON BISESTILI

          when($"data_revoca_movein".isNotNull and datediff(col("data_revoca_movein"), col("data_attivazione")) >= 366 and
            datediff(col("data_revoca_movein"), col("data_attivazione")) <= 379 and //RITARDO MASSIMO IN GIORNI RISCONTRATO AD OGGI
            datediff(col("data_fine_adesione"), col("data_attivazione")) === 366, 366). //DIFFERENZA CON ANNI BISESTILI

          //when( (datediff( col("data_revoca_movein"), col("data_attivazione") ) > 365 ), 0 ).

          otherwise(datediff(col("data_revoca_movein"), col("data_attivazione")))).

      withColumn("giorni_diff", when($"giorni_diff".isNull, 0). //I CASI IN CUI GIORNI DIFF SIANO > 365 (O > 366) SONO RITARDI DI COMUNICAZIONE QUINDI VANNO SANATI COME SEGUE

        when($"giorni_diff" >= 365 and datediff(col("data_fine_adesione"), col("data_attivazione")) === 365, 365). //DIFFERENZA CON ANNI NON BISESTILI

        when($"giorni_diff" >= 366 and datediff(col("data_fine_adesione"), col("data_attivazione")) === 366, 366). //DIFFERENZA CON ANNI BISESTILI

        otherwise($"giorni_diff")).

      withColumn("giorni_anno", when(datediff(col("data_fine_adesione"), col("data_attivazione")) === 365 or
        datediff(col("data_fine_adesione"), col("data_attivazione")) === 366, datediff(col("data_fine_adesione"), col("data_attivazione")))).

      withColumn("perc_compl_del_serv_da_rinnovo", round(lit(($"giorni_diff" / $"giorni_anno") /* *100 */).cast("Double"), 2))

    //withColumn("perc_compl_del_serv_da_rinnovo", concat( round(lit( ($"giorni_diff"/$"giorni_anno")*100).cast("Double"), 2), lit("%") ) )
    //withColumn("perc_compl_del_serv_da_rinnovo", lit( ($"giorni_diff"/$"giorni_anno")*100).cast("Int") )

    df_finale_storico.write.mode("overwrite").format("Parquet").save("df_finale_storico")

    val df_finale_storico_1 = spark.read.load("df_finale_storico")

    //(1)

    //QUESTO PARAGRAFO INSIME AL (2) RISOLVE IL VERIFICARSI DELLA CONDIZIONE ANOMALA IN CUI VENIVA CREATA UNA NUOVA ROW CON VALORI 0 SU GIORNI_DIFF E %_COMPLETAMENTO

    val marge_01 = df_finale_storico_1.select("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp", "data_adesione", "data_attivazione", "data_fine_adesione", "data_revoca_movein", "giorni_diff", "giorni_anno", "perc_compl_del_serv_da_rinnovo").distinct()

    //(2)

    //QUINDI AGGREGHIAMO SULLE DATE UGUALI ED EFFETTUIAMO LA SUM SUI GIORNI_DIFF = 0, %_COMPLETAMENTO = 0

    val marge_02 = marge_01.groupBy("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp", "data_adesione", "data_attivazione", "data_fine_adesione", "data_revoca_movein", "giorni_anno").agg(sum($"giorni_diff").as("giorni_diff"), sum($"perc_compl_del_serv_da_rinnovo").as("perc_compl_del_serv_da_rinnovo"))

    // (V4)

    val marge_03 = marge_02.select("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp", "data_adesione", "data_attivazione", "data_fine_adesione", "data_revoca_movein", "giorni_diff", "giorni_anno", "perc_compl_del_serv_da_rinnovo").
      withColumn("data_revoca_movein", when($"data_revoca_movein".isNull, to_date(lit("1979-01-01"), "yyyy-MM-dd")).otherwise($"data_revoca_movein")).
      distinct()

    // (V4)

    val join_df_finale = join_df_02.join(marge_03, Seq("targa_hmac", "categoria_veicolo", "tipo_adesione", "alimentazione_veicolo", "classe_ambientale", "tsp", "data_adesione", "data_attivazione", "data_fine_adesione", "data_revoca_movein"), "left").orderBy(asc("data_revoca_movein")).distinct()

    join_df_finale.write.mode("overwrite").format("Parquet").save("join_df_finale")

    val join_df_finale_1 = spark.read.load("join_df_finale")

    //******************************************************************************************FINE_DEGLI_STEP_PREP***********************************************************************************************************


    val df_utenti_rinnovati_ad_oggi_01 = join_df_finale.select("targa_hmac", "tipo_adesione", "categoria_veicolo", "alimentazione_veicolo", "classe_ambientale", "tsp", "data_adesione", "data_attivazione", "data_fine_adesione", "data_revoca_movein", "data_riattivazione", "data_raggiungimento_km_max", "km_max_percorribili", "soglia", "km_a1_u_noeco", "km_a1_u_eco", "km_a1_a_noeco", "km_a1_a_eco", "km_a1_e", "km_a2_e", "km_a2_u_eco", "km_a2_u_noeco", "km_a2_a_noeco", "km_a2_a_eco", "di_datafile", "km_tot_eco", "km_tot_noeco", "km_tot_a1", "km_tot_a2", "km_tot_u", "km_tot_a", "km_tot_e", "km_tot_a1_u", "km_tot_a1_a", "km_tot_a2_u", "km_tot_a2_a", "diff_km_prev_day", "km_tot_cumulati", "totale_rinnovi", "totale_revoche", "perc_compl_del_serv_da_rinnovo", "data_oggi", "di_filename", "di_week", "targa", "di_date", "di_run").
      orderBy(desc("di_datafile")).
      withColumnRenamed("km_tot_cumulati", "km_tot_cumulati_fino_ad_oggi").
      withColumn("data_revoca_movein", when($"data_revoca_movein" === "1979-01-01", to_date(lit("0000-00-00"), "yyyy-MM-dd")).otherwise($"data_revoca_movein")).
      distinct() /*.

                                               //SE VOGLIAMO ESSERE MASOCHISTI FACCIAMO QUESTO CAMBIO DI DATE DA US A EU

                                               withColumn("anno", split($"data_adesione", "-").getItem(0)).
                                               withColumn("mese", split($"data_adesione", "-").getItem(1)).
                                               withColumn("giorno", split($"data_adesione", "-").getItem(2)).
                                               withColumn("data_adesione", concat($"giorno", lit("-"),  $"mese", lit("-"), $"anno") ).drop("anno","mese","giorno").

                                               withColumn("anno", split($"data_attivazione", "-").getItem(0)).
                                               withColumn("mese", split($"data_attivazione", "-").getItem(1)).
                                               withColumn("giorno", split($"data_attivazione", "-").getItem(2)).
                                               withColumn("data_attivazione", concat($"giorno", lit("-"),  $"mese", lit("-"), $"anno") ).drop("anno","mese","giorno").

                                               withColumn("anno", split($"data_fine_adesione", "-").getItem(0)).
                                               withColumn("mese", split($"data_fine_adesione", "-").getItem(1)).
                                               withColumn("giorno", split($"data_fine_adesione", "-").getItem(2)).
                                               withColumn("data_fine_adesione", concat($"giorno", lit("-"),  $"mese", lit("-"), $"anno") ).drop("anno","mese","giorno").

                                               withColumn("anno", split($"data_oggi", "-").getItem(0)).
                                               withColumn("mese", split($"data_oggi", "-").getItem(1)).
                                               withColumn("giorno", split($"data_oggi", "-").getItem(2)).
                                               withColumn("data_oggi", concat($"giorno", lit("-"),  $"mese", lit("-"), $"anno") ).drop("anno","mese","giorno")*/


    val data = data_oggi.replace("-", "_")

    //Salvo la tabella in staging

    hive.executeUpdate(s"drop table if exists $stg.situazione_utenti_rinnovati_V5_4_ad_oggi_$data")

    df_utenti_rinnovati_ad_oggi_01.write.format("orc").saveAsTable(s"""$stg.situazione_utenti_rinnovati_V5_4_ad_oggi_$data""")


  }


}