-- DDL Schema for Deep Nested JSON (9 levels of nesting)

`Op` STRING,
`customer_id` INT,
`first_name` STRING,
`last_name` STRING,
`addresses` ARRAY<STRUCT<
  `type`: STRING,
  `street`: STRING,
  `city`: STRING,
  `details`: STRUCT<
    `zip`: STRING,
    `country`: STRING,
    `coordinates`: STRUCT<
      `latitude`: DOUBLE,
      `longitude`: DOUBLE,
      `elevation`: STRUCT<
        `meters`: DOUBLE,
        `feet`: DOUBLE,
        `accuracy`: STRUCT<
          `gps_quality`: STRING,
          `source`: STRING,
          `metadata`: STRUCT<
            `timestamp`: STRING,
            `device_info`: STRUCT<
              `manufacturer`: STRING,
              `model`: STRING,
              `firmware`: STRING,
              `calibration`: STRUCT<
                `last_updated`: STRING,
                `accuracy_rating`: DOUBLE,
                `verification_status`: STRUCT<
                  `verified`: BOOLEAN,
                  `verified_by`: STRING,
                  `certificate_details`: STRUCT<
                    `cert_id`: STRING,
                    `valid_until`: STRING,
                    `issuing_authority`: STRUCT<
                      `name`: STRING,
                      `country`: STRING,
                      `contact_info`: STRUCT<
                        `email`: STRING,
                        `phone`: STRING,
                        `address`: STRUCT<
                          `street`: STRING,
                          `city`: STRING,
                          `postal_code`: STRING,
                          `building_details`: STRUCT<
                            `floor`: INT,
                            `suite`: STRING,
                            `building_name`: STRING
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
    >,
    `timezone`: STRING,
    `postal_service`: STRUCT<
      `delivery_zone`: STRING,
      `mail_carrier`: STRUCT<
        `name`: STRING,
        `employee_id`: STRING,
        `route_details`: STRUCT<
          `route_number`: STRING,
          `delivery_schedule`: STRUCT<
            `monday`: STRING,
            `tuesday`: STRING,
            `wednesday`: STRING,
            `thursday`: STRING,
            `friday`: STRING,
            `weekend_service`: STRUCT<
              `saturday`: STRUCT<
                `available`: BOOLEAN,
                `hours`: STRING,
                `special_instructions`: STRUCT<
                  `requires_signature`: BOOLEAN,
                  `package_handling`: STRUCT<
                    `max_weight_kg`: INT,
                    `fragile_items`: BOOLEAN,
                    `refrigerated`: BOOLEAN,
                    `delivery_preferences`: STRUCT<
                      `preferred_location`: STRING,
                      `backup_location`: STRING,
                      `neighbor_delivery`: STRUCT<
                        `allowed`: BOOLEAN,
                        `authorized_neighbors`: ARRAY<STRUCT<
                          `name`: STRING,
                          `address`: STRING,
                          `phone`: STRING,
                          `relationship`: STRING,
                          `authorization_details`: STRUCT<
                            `signed_date`: STRING,
                            `expires`: STRING,
                            `document_id`: STRING,
                            `witness_info`: STRUCT<
                              `witness_name`: STRING,
                              `witness_signature`: STRING,
                              `notary_details`: STRUCT<
                                `notary_name`: STRING,
                                `notary_id`: STRING,
                                `seal_number`: STRING,
                                `commission_expires`: STRING
                              >
                            >
                          >
                        >>
                      >
                    >
                  >
                >
              >,
              `sunday`: STRUCT<
                `available`: BOOLEAN
              >
            >
          >
        >
      >
    >,
    `building_access`: STRUCT<
      `security_level`: STRING,
      `access_card_required`: BOOLEAN,
      `visitor_management`: STRUCT<
        `reception_desk`: STRUCT<
          `floor`: INT,
          `hours`: STRING,
          `staff`: ARRAY<STRUCT<
            `name`: STRING,
            `shift`: STRING,
            `languages`: ARRAY<STRING>,
            `certifications`: STRUCT<
              `security_clearance`: STRING,
              `first_aid`: STRING,
              `training_records`: STRUCT<
                `last_training`: STRING,
                `next_required`: STRING,
                `training_provider`: STRUCT<
                  `company`: STRING,
                  `instructor`: STRING,
                  `course_details`: STRUCT<
                    `course_id`: STRING,
                    `duration_hours`: INT,
                    `modules_completed`: ARRAY<STRUCT<
                      `module_name`: STRING,
                      `completion_date`: STRING,
                      `score`: INT,
                      `practical_assessment`: STRUCT<
                        `passed`: BOOLEAN,
                        `assessor`: STRING,
                        `assessment_details`: STRUCT<
                          `scenario_type`: STRING,
                          `response_time_seconds`: INT,
                          `accuracy_score`: DOUBLE,
                          `areas_for_improvement`: ARRAY<STRING>
                        >
                      >
                    >>
                  >
                >
              >
            >
          >>
        >
      >
    >
  >
>>,
`profile`: STRUCT<
  `account_status`: STRING,
  `membership_tier`: STRING,
  `preferences`: STRUCT<
    `communication`: STRUCT<
      `email_notifications`: BOOLEAN,
      `sms_alerts`: BOOLEAN,
      `push_notifications`: STRUCT<
        `enabled`: BOOLEAN,
        `categories`: STRUCT<
          `promotional`: BOOLEAN,
          `security`: BOOLEAN,
          `billing`: BOOLEAN,
          `service_updates`: STRUCT<
            `maintenance`: BOOLEAN,
            `feature_releases`: BOOLEAN,
            `policy_changes`: BOOLEAN,
            `notification_settings`: STRUCT<
              `frequency`: STRING,
              `delivery_method`: STRING,
              `fallback_methods`: ARRAY<STRUCT<
                `method`: STRING,
                `priority`: INT,
                `conditions`: STRUCT<
                  `mobile_unavailable`: BOOLEAN,
                  `retry_settings`: STRUCT<
                    `max_retries`: INT,
                    `retry_interval_minutes`: INT,
                    `escalation_policy`: STRUCT<
                      `escalate_after_failures`: INT,
                      `escalation_contacts`: ARRAY<STRUCT<
                        `contact_type`: STRING,
                        `address`: STRING,
                        `verification_status`: STRING,
                        `added_date`: STRING,
                        `verification_details`: STRUCT<
                          `verification_code_sent`: STRING,
                          `verified_at`: STRING,
                          `verification_method`: STRING,
                          `ip_address`: STRING,
                          `user_agent`: STRING,
                          `device_fingerprint`: STRUCT<
                            `browser`: STRING,
                            `os`: STRING,
                            `screen_resolution`: STRING,
                            `timezone`: STRING,
                            `language`: STRING
                          >
                        >
                      >>
                    >
                  >
                >
              >>
            >
          >
        >
      >
    >
  >
>