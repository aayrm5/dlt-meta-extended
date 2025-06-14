STRUCT<
  `Op` STRING,
  `customer_id` INT,
  `first_name` STRING,
  `addresses` ARRAY<
    STRUCT<
      `type` STRING,
      `street` STRING,
      `city` STRING,
      `details` STRUCT<
        `zip` STRING,
        `country` STRING,
        `contact_info` STRUCT<
          `phone` STRUCT<
            `primary` STRING,
            `emergency` STRING,
            `preferences` STRUCT<
              `call_time` STRING,
              `notification_settings` STRUCT<
                `sms` BOOLEAN,
                `email` BOOLEAN,
                `push` STRUCT<
                  `enabled` BOOLEAN,
                  `frequency` STRING,
                  `custom_rules` STRUCT<
                    `weekend` BOOLEAN,
                    `holidays` BOOLEAN,
                    `advanced_filters` STRUCT<
                      `location_based` BOOLEAN,
                      `time_zone_aware` BOOLEAN,
                      `user_behavior_analysis` STRUCT<
                        `tracking_enabled` BOOLEAN,
                        `data_retention_days` INT,
                        `privacy_settings` STRUCT<
                          `anonymize_data` BOOLEAN,
                          `third_party_sharing` BOOLEAN,
                          `encryption_level` STRING
                        >
                      >
                    >
                  >
                >
              >
            >
          >
        >
      >
    >
  >
>