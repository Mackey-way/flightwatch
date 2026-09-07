/*M!999999\- enable the sandbox mode */ 

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*M!100616 SET @OLD_NOTE_VERBOSITY=@@NOTE_VERBOSITY, NOTE_VERBOSITY=0 */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `airline_rules` (
  `airline_iata` char(2) NOT NULL,
  `airline_name` varchar(60) DEFAULT NULL,
  `desk_close_min` smallint(5) unsigned NOT NULL DEFAULT 45,
  `online_close_min` smallint(5) unsigned NOT NULL DEFAULT 60,
  `gate_close_min` smallint(5) unsigned NOT NULL DEFAULT 20,
  `anchor_desk` enum('scheduled','estimated') NOT NULL DEFAULT 'scheduled',
  `source_note` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`airline_iata`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `airport_rules` (
  `airport` char(3) NOT NULL,
  `name` varchar(60) DEFAULT NULL,
  `utc_offset_min` smallint(6) DEFAULT NULL COMMENT 'реальное смещение от UTC; нужно, чтобы сводить местное время табло с меткой наблюдения',
  `desk_open_min` smallint(5) unsigned DEFAULT NULL COMMENT 'за сколько до вылета открывается регистрация',
  `desk_close_min` smallint(5) unsigned DEFAULT NULL COMMENT 'закрытие стойки по правилам АЭРОПОРТА; перевозчик может ужесточить',
  `anchor_open` enum('scheduled','estimated') NOT NULL DEFAULT 'scheduled',
  `be_at_airport_min` smallint(5) unsigned DEFAULT NULL COMMENT 'официальная рекомендация "быть в аэропорту не позднее"',
  `security_minutes` smallint(5) unsigned DEFAULT NULL COMMENT 'типовое время на контроль',
  `note` varchar(200) DEFAULT NULL,
  `rule_url` varchar(200) DEFAULT NULL,
  PRIMARY KEY (`airport`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `airport_status` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `airport` char(3) NOT NULL,
  `checked_at` datetime(3) NOT NULL,
  `flights` smallint(5) unsigned NOT NULL COMMENT 'рейсов в окне оценки',
  `avg_delay` int(11) DEFAULT NULL,
  `max_delay` int(11) DEFAULT NULL,
  `cancelled` smallint(5) unsigned NOT NULL DEFAULT 0,
  `share_bad` decimal(5,2) DEFAULT NULL COMMENT 'доля рейсов с задержкой >= 60 мин',
  `state` enum('norm','stress','outage') NOT NULL,
  `changed` tinyint(1) NOT NULL DEFAULT 0 COMMENT '1 = в этот момент состояние сменилось',
  PRIMARY KEY (`id`),
  KEY `ix_ap` (`airport`,`checked_at`)
) ENGINE=InnoDB AUTO_INCREMENT=3810 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `connect_defaults` (
  `layover_type` enum('airside','landside') NOT NULL,
  `baggage_mode` enum('no_bag','through_checked','reclaim_recheck') NOT NULL,
  `mct_minutes` smallint(5) unsigned NOT NULL,
  `note` varchar(160) DEFAULT NULL,
  PRIMARY KEY (`layover_type`,`baggage_mode`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `flight_history` (
  `fr24_id` varchar(16) NOT NULL,
  `flight_no` varchar(10) DEFAULT NULL,
  `callsign` varchar(12) DEFAULT NULL,
  `reg` varchar(12) DEFAULT NULL,
  `aircraft_type` varchar(8) DEFAULT NULL,
  `orig_icao` char(4) DEFAULT NULL,
  `dest_icao` char(4) DEFAULT NULL,
  `dest_icao_actual` char(4) DEFAULT NULL,
  `takeoff_utc` datetime DEFAULT NULL,
  `landed_utc` datetime DEFAULT NULL,
  `first_seen` datetime DEFAULT NULL,
  `last_seen` datetime DEFAULT NULL,
  `ended` tinyint(1) DEFAULT NULL,
  `loaded_at` datetime(3) NOT NULL,
  PRIMARY KEY (`fr24_id`),
  KEY `ix_no` (`flight_no`,`takeoff_utc`),
  KEY `ix_reg` (`reg`,`takeoff_utc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `flight_state` (
  `flight_id` int(10) unsigned NOT NULL,
  `regime` enum('city','connection') NOT NULL DEFAULT 'city',
  `status_class` varchar(24) DEFAULT NULL,
  `scheduled_local` datetime DEFAULT NULL,
  `estimated_local` datetime DEFAULT NULL,
  `actual_local` datetime DEFAULT NULL,
  `tz_offset_min` smallint(6) NOT NULL DEFAULT 0,
  `delay_min` int(11) DEFAULT NULL,
  `gate` varchar(12) DEFAULT NULL,
  `gate_changed_at` datetime DEFAULT NULL,
  `checkin_desks` varchar(24) DEFAULT NULL,
  `be_at_airport_local` datetime DEFAULT NULL,
  `binding_deadline` enum('desk','online','gate') DEFAULT NULL,
  `binding_anchor` enum('scheduled','estimated') DEFAULT NULL,
  `desk_required` tinyint(1) DEFAULT NULL COMMENT 'нужен ли физический поход к стойке',
  `leave_at_local` datetime DEFAULT NULL,
  `connect_buffer_min` int(11) DEFAULT NULL COMMENT 'минут запаса на стыковку',
  `connect_risk` enum('ok','tight','broken') DEFAULT NULL,
  `inbound_actual_local` datetime DEFAULT NULL COMMENT 'фактический прилёт входящего',
  `bag_belt` varchar(12) DEFAULT NULL COMMENT 'лента входящего: признак, что багаж пошёл',
  `rotation_reg` varchar(12) DEFAULT NULL,
  `rotation_flight` varchar(10) DEFAULT NULL COMMENT 'где борт сейчас',
  `rotation_eta_utc` datetime DEFAULT NULL COMMENT 'когда борт будет в аэропорту вылета',
  `predicted_dep_local` datetime DEFAULT NULL COMMENT 'не раньше чем: прилёт борта + стоянка',
  `rotation_risk_min` int(11) DEFAULT NULL COMMENT 'на сколько прогноз хуже расписания',
  `wake_at_local` datetime DEFAULT NULL,
  `conflict` tinyint(1) NOT NULL DEFAULT 0,
  `degraded_sources` varchar(120) DEFAULT NULL,
  `updated_at` datetime(3) NOT NULL,
  PRIMARY KEY (`flight_id`),
  CONSTRAINT `fk_state_flight` FOREIGN KEY (`flight_id`) REFERENCES `flights` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `flights` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `slug` varchar(80) NOT NULL,
  `owner` enum('self','meeting') NOT NULL DEFAULT 'self' COMMENT 'чей рейс: свой или чужой, за которым следим ради встречи',
  `label` varchar(40) DEFAULT NULL COMMENT 'метка в тексте оповещений',
  `itinerary` varchar(40) DEFAULT NULL COMMENT 'бронь/PNR, объединяет сегменты',
  `segment_no` tinyint(3) unsigned DEFAULT NULL,
  `inbound_flight_id` int(10) unsigned DEFAULT NULL COMMENT 'предыдущий сегмент, если это стыковка',
  `flight_no` varchar(10) NOT NULL,
  `flight_date` date NOT NULL,
  `airport` char(3) NOT NULL,
  `peer_airport` char(3) DEFAULT NULL,
  `direction` enum('departure','arrival') NOT NULL DEFAULT 'departure',
  `airline_iata` char(2) DEFAULT NULL,
  `aircraft_reg` varchar(12) DEFAULT NULL COMMENT 'борт, которым обычно выполняется рейс',
  `rotation_inbound_no` varchar(10) DEFAULT NULL COMMENT 'рейс, которым борт приходит в аэропорт вылета',
  `turnaround_min` smallint(5) unsigned NOT NULL DEFAULT 60 COMMENT 'минимальная стоянка между прилётом борта и вылетом нашего рейса',
  `has_boarding_pass` tinyint(1) NOT NULL DEFAULT 0,
  `checked_baggage` tinyint(1) NOT NULL DEFAULT 0,
  `baggage_mode` enum('no_bag','through_checked','reclaim_recheck') NOT NULL DEFAULT 'no_bag',
  `layover_type` enum('none','airside','landside') NOT NULL DEFAULT 'none',
  `min_connect_minutes` smallint(5) unsigned DEFAULT NULL COMMENT 'MCT: airside меньше, с получением багажа заметно больше',
  `travel_minutes` smallint(5) unsigned DEFAULT NULL,
  `personal_buffer_minutes` smallint(5) unsigned NOT NULL DEFAULT 20,
  `sms_to` varchar(20) DEFAULT NULL,
  `extra_sms_to` varchar(200) DEFAULT NULL COMMENT 'доп. номера ЧЕРЕЗ ЗАПЯТУЮ; основной получает всё всегда и здесь не указывается',
  `notify` tinyint(1) NOT NULL DEFAULT 1,
  `created_at` datetime NOT NULL DEFAULT current_timestamp(),
  `retired_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_slug` (`slug`),
  KEY `ix_date` (`flight_date`),
  KEY `ix_airport` (`airport`),
  KEY `ix_itinerary` (`itinerary`,`segment_no`),
  KEY `ix_inbound` (`inbound_flight_id`)
) ENGINE=InnoDB AUTO_INCREMENT=492 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `notifications` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `flight_id` int(10) unsigned DEFAULT NULL,
  `event_class` varchar(32) NOT NULL,
  `severity` enum('wake','digest','info') NOT NULL,
  `dedup_key` varchar(140) NOT NULL,
  `body` text NOT NULL,
  `expires_at` datetime DEFAULT NULL,
  `sms_req_id` varchar(40) DEFAULT NULL,
  `sms_sent_at` datetime(3) DEFAULT NULL,
  `sms_bridge_ok` tinyint(1) DEFAULT NULL,
  `tg_sent_at` datetime(3) DEFAULT NULL,
  `tg_message_id` bigint(20) DEFAULT NULL,
  `tg_repeats` smallint(5) unsigned NOT NULL DEFAULT 0,
  `acked_at` datetime(3) DEFAULT NULL,
  `created_at` datetime(3) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_dedup` (`dedup_key`),
  KEY `ix_pending` (`acked_at`,`severity`,`created_at`),
  KEY `fk_note_flight` (`flight_id`),
  CONSTRAINT `fk_note_flight` FOREIGN KEY (`flight_id`) REFERENCES `flights` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB AUTO_INCREMENT=42 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `observations` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `flight_id` int(10) unsigned NOT NULL,
  `source` varchar(20) NOT NULL,
  `jurisdiction` enum('origin','destination','aggregator') NOT NULL,
  `leg` enum('DEP','ARR') NOT NULL,
  `status_raw` varchar(120) DEFAULT NULL,
  `status_class` varchar(24) DEFAULT NULL,
  `scheduled_local` datetime DEFAULT NULL,
  `estimated_local` datetime DEFAULT NULL,
  `actual_local` datetime DEFAULT NULL,
  `tz_offset_min` smallint(6) NOT NULL DEFAULT 0,
  `gate` varchar(12) DEFAULT NULL,
  `checkin_desks` varchar(24) DEFAULT NULL,
  `carousel` varchar(12) DEFAULT NULL,
  `terminal` varchar(12) DEFAULT NULL,
  `peer_iata` char(3) DEFAULT NULL,
  `payload_hash` char(32) NOT NULL,
  `first_seen_at` datetime(3) NOT NULL,
  `last_seen_at` datetime(3) NOT NULL,
  `seen_count` int(10) unsigned NOT NULL DEFAULT 1,
  PRIMARY KEY (`id`),
  KEY `ix_latest` (`flight_id`,`source`,`leg`,`first_seen_at`),
  KEY `ix_hash` (`flight_id`,`source`,`leg`,`payload_hash`),
  CONSTRAINT `fk_obs_flight` FOREIGN KEY (`flight_id`) REFERENCES `flights` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=156 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `predictions` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `flight_id` int(10) unsigned DEFAULT NULL COMMENT 'NULL у теневых рейсов, которые мы наблюдаем без оповещений',
  `flight_no` varchar(10) NOT NULL,
  `flight_date` date NOT NULL,
  `made_at` datetime(3) NOT NULL,
  `horizon_min` int(11) NOT NULL COMMENT 'за сколько минут до планового вылета сделан прогноз',
  `scheduled_local` datetime DEFAULT NULL,
  `predicted_delay_min` int(11) NOT NULL,
  `predicted_dep_local` datetime DEFAULT NULL,
  `method` varchar(24) NOT NULL,
  `components` text DEFAULT NULL COMMENT 'вклад каждого сигнала, для разбора ошибок',
  `board_delay_min` int(11) DEFAULT NULL COMMENT 'что показывало табло в этот момент',
  `actual_delay_min` int(11) DEFAULT NULL,
  `error_min` int(11) DEFAULT NULL,
  `settled_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_flight` (`flight_id`,`made_at`),
  KEY `ix_unsettled` (`settled_at`,`flight_date`)
) ENGINE=InnoDB AUTO_INCREMENT=204 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `punctuality` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `flight_no` varchar(10) NOT NULL,
  `airport` char(3) NOT NULL,
  `flight_date` date NOT NULL,
  `scheduled_local` datetime DEFAULT NULL,
  `actual_local` datetime DEFAULT NULL,
  `delay_min` int(11) DEFAULT NULL,
  `status_class` varchar(24) DEFAULT NULL,
  `aircraft_reg` varchar(12) DEFAULT NULL,
  `source` varchar(20) DEFAULT NULL,
  `updated_at` datetime(3) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_day` (`flight_no`,`airport`,`flight_date`),
  KEY `ix_no` (`flight_no`,`flight_date`)
) ENGINE=InnoDB AUTO_INCREMENT=32015 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `rotation_log` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `aircraft_reg` varchar(12) NOT NULL,
  `seen_at` datetime(3) NOT NULL,
  `flight_no` varchar(10) DEFAULT NULL,
  `origin` char(3) DEFAULT NULL,
  `destination` char(3) DEFAULT NULL,
  `eta_utc` datetime DEFAULT NULL,
  `landed_utc` datetime DEFAULT NULL,
  `status` varchar(40) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_reg` (`aircraft_reg`,`seen_at`)
) ENGINE=InnoDB AUTO_INCREMENT=186 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `source_polls` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `source` varchar(20) NOT NULL,
  `airport` char(3) NOT NULL,
  `ok` tinyint(1) NOT NULL,
  `not_modified` tinyint(1) NOT NULL DEFAULT 0,
  `rows_returned` int(11) DEFAULT NULL,
  `changed_rows` int(11) DEFAULT NULL,
  `duration_ms` int(11) DEFAULT NULL,
  `error` varchar(255) DEFAULT NULL,
  `polled_at` datetime(3) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `ix_health` (`source`,`airport`,`polled_at`)
) ENGINE=InnoDB AUTO_INCREMENT=16757 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `travel_profiles` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `airport` char(3) NOT NULL,
  `from_hour` tinyint(3) unsigned NOT NULL,
  `to_hour` tinyint(3) unsigned NOT NULL COMMENT 'верхняя граница не включается',
  `minutes` smallint(5) unsigned NOT NULL,
  `confirmed` tinyint(1) NOT NULL DEFAULT 0 COMMENT '0 = предположение, не подтверждено',
  `note` varchar(160) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_band` (`airport`,`from_hour`),
  KEY `ix_airport` (`airport`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `v_board_disagreement` AS SELECT
 1 AS `рейс`,
  1 AS `дата`,
  1 AS `у_вылета`,
  1 AS `у_прилёта`,
  1 AS `агрегатор`,
  1 AS `проверено` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `v_flight_timeline` AS SELECT
 1 AS `рейс`,
  1 AS `дата`,
  1 AS `источник`,
  1 AS `юрисдикция`,
  1 AS `плечо`,
  1 AS `класс`,
  1 AS `статус`,
  1 AS `план`,
  1 AS `оценка`,
  1 AS `задержка_мин`,
  1 AS `гейт`,
  1 AS `стойки`,
  1 AS `впервые`,
  1 AS `последний_раз`,
  1 AS `раз` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `v_msk_stress` AS SELECT
 1 AS `дата`,
  1 AS `рейсов`,
  1 AS `средняя`,
  1 AS `макс`,
  1 AS `тяжёлых`,
  1 AS `оценка` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `v_prediction_accuracy` AS SELECT
 1 AS `рейс`,
  1 AS `дата`,
  1 AS `горизонт_ч`,
  1 AS `прогноз`,
  1 AS `табло`,
  1 AS `факт`,
  1 AS `ошибка`,
  1 AS `метод`,
  1 AS `сделан` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `v_punctuality` AS SELECT
 1 AS `рейс`,
  1 AS `порт`,
  1 AS `наблюдений`,
  1 AS `с_фактом`,
  1 AS `вовремя`,
  1 AS `средняя_задержка`,
  1 AS `худшая`,
  1 AS `лучшая`,
  1 AS `по_дням` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `v_source_health` AS SELECT
 1 AS `источник`,
  1 AS `аэропорт`,
  1 AS `опросов`,
  1 AS `отказов`,
  1 AS `процент_отказов`,
  1 AS `средн_мс`,
  1 AS `макс_мс`,
  1 AS `последний_успех`,
  1 AS `последняя_ошибка` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `v_warning_time` AS SELECT
 1 AS `рейс`,
  1 AS `дата`,
  1 AS `аэропорт`,
  1 AS `источник`,
  1 AS `статус`,
  1 AS `план`,
  1 AS `оценка`,
  1 AS `сдвиг_мин`,
  1 AS `впервые_msk`,
  1 AS `фора_мин` */;
SET character_set_client = @saved_cs_client;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `wave_stats` (
  `airport` char(3) NOT NULL,
  `stat_date` date NOT NULL,
  `hour` tinyint(3) unsigned NOT NULL,
  `leg` enum('DEP','ARR') NOT NULL,
  `airline` varchar(3) NOT NULL DEFAULT '' COMMENT 'пусто = все перевозчики',
  `flights` smallint(5) unsigned NOT NULL,
  `avg_delay` int(11) DEFAULT NULL,
  `max_delay` int(11) DEFAULT NULL,
  `on_time` smallint(5) unsigned DEFAULT NULL COMMENT 'сколько уложились в 15 минут',
  `updated_at` datetime(3) NOT NULL,
  PRIMARY KEY (`airport`,`stat_date`,`hour`,`leg`,`airline`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50001 DROP VIEW IF EXISTS `v_board_disagreement`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `v_board_disagreement` AS select `f`.`flight_no` AS `рейс`,`f`.`flight_date` AS `дата`,max(case when `o`.`jurisdiction` = 'origin' then `o`.`status_class` end) AS `у_вылета`,max(case when `o`.`jurisdiction` = 'destination' then `o`.`status_class` end) AS `у_прилёта`,max(case when `o`.`jurisdiction` = 'aggregator' then `o`.`status_class` end) AS `агрегатор`,max(`o`.`last_seen_at`) AS `проверено` from (`observations` `o` join `flights` `f` on(`f`.`id` = `o`.`flight_id`)) where `o`.`last_seen_at` > current_timestamp() - interval 1 hour group by `f`.`id` having count(distinct `o`.`status_class`) > 1 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `v_flight_timeline`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `v_flight_timeline` AS select `f`.`flight_no` AS `рейс`,`f`.`flight_date` AS `дата`,`o`.`source` AS `источник`,`o`.`jurisdiction` AS `юрисдикция`,`o`.`leg` AS `плечо`,`o`.`status_class` AS `класс`,`o`.`status_raw` AS `статус`,cast(`o`.`scheduled_local` as time) AS `план`,cast(`o`.`estimated_local` as time) AS `оценка`,timestampdiff(MINUTE,`o`.`scheduled_local`,`o`.`estimated_local`) AS `задержка_мин`,`o`.`gate` AS `гейт`,`o`.`checkin_desks` AS `стойки`,`o`.`first_seen_at` AS `впервые`,`o`.`last_seen_at` AS `последний_раз`,`o`.`seen_count` AS `раз` from (`observations` `o` join `flights` `f` on(`f`.`id` = `o`.`flight_id`)) order by `f`.`flight_date`,`f`.`flight_no`,`o`.`first_seen_at` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `v_msk_stress`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `v_msk_stress` AS select `punctuality`.`flight_date` AS `дата`,count(0) AS `рейсов`,round(avg(`punctuality`.`delay_min`),0) AS `средняя`,max(`punctuality`.`delay_min`) AS `макс`,sum(`punctuality`.`delay_min` >= 120) AS `тяжёлых`,case when avg(`punctuality`.`delay_min`) >= 90 then 'СБОЙ' when avg(`punctuality`.`delay_min`) >= 45 then 'напряжённо' else 'норма' end AS `оценка` from `punctuality` where `punctuality`.`flight_no` in ('XX1234','YY5678') and `punctuality`.`delay_min` is not null group by `punctuality`.`flight_date` order by `punctuality`.`flight_date` desc */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `v_prediction_accuracy`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `v_prediction_accuracy` AS select `predictions`.`flight_no` AS `рейс`,`predictions`.`flight_date` AS `дата`,round(`predictions`.`horizon_min` / 60,1) AS `горизонт_ч`,`predictions`.`predicted_delay_min` AS `прогноз`,`predictions`.`board_delay_min` AS `табло`,`predictions`.`actual_delay_min` AS `факт`,`predictions`.`error_min` AS `ошибка`,`predictions`.`method` AS `метод`,`predictions`.`made_at` AS `сделан` from `predictions` order by `predictions`.`flight_date`,`predictions`.`horizon_min` desc */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `v_punctuality`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `v_punctuality` AS select `punctuality`.`flight_no` AS `рейс`,`punctuality`.`airport` AS `порт`,count(0) AS `наблюдений`,sum(`punctuality`.`delay_min` is not null) AS `с_фактом`,sum(`punctuality`.`delay_min` <= 15) AS `вовремя`,round(avg(`punctuality`.`delay_min`),0) AS `средняя_задержка`,max(`punctuality`.`delay_min`) AS `худшая`,min(`punctuality`.`delay_min`) AS `лучшая`,group_concat(concat(date_format(`punctuality`.`flight_date`,'%d.%m'),':',ifnull(concat('+',`punctuality`.`delay_min`,'м'),'?')) order by `punctuality`.`flight_date` ASC separator '  ') AS `по_дням` from `punctuality` group by `punctuality`.`flight_no`,`punctuality`.`airport` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `v_source_health`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `v_source_health` AS select `source_polls`.`source` AS `источник`,`source_polls`.`airport` AS `аэропорт`,count(0) AS `опросов`,sum(`source_polls`.`ok` = 0) AS `отказов`,round(100 * sum(`source_polls`.`ok` = 0) / count(0),1) AS `процент_отказов`,round(avg(nullif(`source_polls`.`duration_ms`,0)),0) AS `средн_мс`,max(`source_polls`.`duration_ms`) AS `макс_мс`,max(case when `source_polls`.`ok` = 1 then `source_polls`.`polled_at` end) AS `последний_успех`,substring_index(group_concat(`source_polls`.`error` order by `source_polls`.`id` DESC separator ' | '),' | ',1) AS `последняя_ошибка` from `source_polls` group by `source_polls`.`source`,`source_polls`.`airport` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `v_warning_time`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `v_warning_time` AS select `f`.`flight_no` AS `рейс`,`f`.`flight_date` AS `дата`,`f`.`airport` AS `аэропорт`,`o`.`source` AS `источник`,`o`.`status_raw` AS `статус`,cast(`o`.`scheduled_local` as time) AS `план`,cast(`o`.`estimated_local` as time) AS `оценка`,timestampdiff(MINUTE,`o`.`scheduled_local`,`o`.`estimated_local`) AS `сдвиг_мин`,`o`.`first_seen_at` AS `впервые_msk`,timestampdiff(MINUTE,`o`.`first_seen_at` + interval ifnull(`ar`.`utc_offset_min`,180) - 180 minute,`o`.`scheduled_local`) AS `фора_мин` from ((`observations` `o` join `flights` `f` on(`f`.`id` = `o`.`flight_id`)) left join `airport_rules` `ar` on(`ar`.`airport` = `f`.`airport`)) where `o`.`estimated_local` is not null and `o`.`scheduled_local` is not null and `o`.`estimated_local` <> `o`.`scheduled_local` order by `o`.`first_seen_at` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*M!100616 SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY */;

