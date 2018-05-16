DROP TABLE IF EXISTS server_settings;
DROP TABLE IF EXISTS bug_tracking_system CASCADE;
DROP TABLE IF EXISTS defect_form_field CASCADE;
DROP TABLE IF EXISTS defect_field_allowed_value CASCADE;
DROP TABLE IF EXISTS bug_tracking_system_auth CASCADE;

DROP TABLE IF EXISTS project_email_configuration CASCADE;
DROP TABLE IF EXISTS project_configuration CASCADE;
DROP TABLE IF EXISTS issue_type_project_configuration CASCADE;
DROP TABLE IF EXISTS issue_type CASCADE;

DROP TABLE IF EXISTS project CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS project_user CASCADE;
DROP TABLE IF EXISTS oauth_access_token CASCADE;
DROP TABLE IF EXISTS oauth_registration_scope;
DROP TABLE IF EXISTS oauth_registration CASCADE;

DROP TABLE IF EXISTS dashboard CASCADE;
DROP TABLE IF EXISTS widget CASCADE;
DROP TABLE IF EXISTS dashboard_widget CASCADE;
DROP TABLE IF EXISTS filter CASCADE;
DROP TABLE IF EXISTS filter_condition CASCADE;
DROP TABLE IF EXISTS filter_order CASCADE;
DROP TABLE IF EXISTS widget_filter CASCADE;
DROP TABLE IF EXISTS widget_options CASCADE;

DROP TABLE IF EXISTS test_item_structure CASCADE;
DROP TABLE IF EXISTS test_item_results CASCADE;

DROP TABLE IF EXISTS issue CASCADE;
DROP TABLE IF EXISTS issue_ticket CASCADE;
DROP TABLE IF EXISTS ticket CASCADE;
DROP TABLE IF EXISTS defect_form_field_value CASCADE;
DROP TABLE IF EXISTS parameter CASCADE;

DROP TRIGGER IF EXISTS last_launch_number_trigger
ON launch;
DROP FUNCTION update_last_launch_number();
DROP TABLE IF EXISTS launch CASCADE;
DROP TABLE IF EXISTS launch_tag CASCADE;
DROP TABLE IF EXISTS item_tag CASCADE;

DROP TABLE IF EXISTS test_item CASCADE;
DROP TABLE IF EXISTS log CASCADE;
DROP TABLE IF EXISTS integration CASCADE;
DROP TABLE IF EXISTS integration_type CASCADE;
DROP TABLE IF EXISTS activity CASCADE;

DROP FUNCTION IF EXISTS check_wired_tickets();
DROP TRIGGER IF EXISTS after_ticket_delete
ON issue_ticket;


DROP TYPE IF EXISTS PROJECT_TYPE_ENUM CASCADE;
DROP TYPE IF EXISTS USER_ROLE_ENUM CASCADE;
DROP TYPE IF EXISTS USER_TYPE_ENUM CASCADE;
DROP TYPE IF EXISTS PROJECT_ROLE_ENUM CASCADE;
DROP TYPE IF EXISTS LAUNCH_MODE_ENUM CASCADE;
DROP TYPE IF EXISTS STATUS_ENUM CASCADE;
DROP TYPE IF EXISTS BTS_TYPE_ENUM CASCADE;
DROP TYPE IF EXISTS AUTH_TYPE_ENUM CASCADE;
DROP TYPE IF EXISTS TEST_ITEM_TYPE_ENUM CASCADE;
DROP TYPE IF EXISTS ISSUE_GROUP_ENUM CASCADE;
DROP TYPE IF EXISTS ACCESS_TOKEN_TYPE_ENUM;
DROP TYPE IF EXISTS ACTIVITY_ENTITY_ENUM;
DROP TYPE IF EXISTS INTEGRATION_AUTH_FLOW_ENUM;
DROP TYPE IF EXISTS INTEGRATION_GROUP_ENUM;
DROP TYPE IF EXISTS FILTER_CONDITION;


