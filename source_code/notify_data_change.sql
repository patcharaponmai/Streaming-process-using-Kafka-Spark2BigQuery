-- PostgreSQL notification function to send notify to my channel
CREATE OR REPLACE FUNCTION notify_data_change() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('data_change2kafka', row_to_json(NEW)::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- PostgreSQL trigger to activate the notification function
CREATE TRIGGER data_change2kafka_trigger
AFTER INSERT ON online_shopping_kafka_spark2bigquery
FOR EACH ROW
EXECUTE FUNCTION notify_data_change();
