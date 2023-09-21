#include <iostream>
#include <cpprest/http_listener.h>
#include <cpprest/json.h>
#include <librdkafka/rdkafka.h>

using namespace web;
using namespace web::http;
using namespace web::http::experimental::listener;

rd_kafka_t* kafka_producer = nullptr;
rd_kafka_topic_t* kafka_topic = nullptr;
rd_kafka_t* kafka_consumer = nullptr;

class RestServer {
public:
    http_listener listener;

    RestServer(const utility::string_t& url)
        : listener(url) {
        listener.support(methods::GET, std::bind(&RestServer::handle_get, this, std::placeholders::_1));
        listener.support(methods::POST, std::bind(&RestServer::handle_post, this, std::placeholders::_1));

        InitializeKafkaProducer();
        InitializeKafkaConsumer();
    }

    void InitializeKafkaProducer() {
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092", nullptr, 0);
        kafka_producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, nullptr, 0);

        if (!kafka_producer) {
            std::cerr << "Failed to create Kafka producer" << std::endl;
            exit(1);
        }

        if (rd_kafka_brokers_add(kafka_producer, "localhost:9092") == 0) {
            std::cerr << "Failed to add brokers to Kafka producer" << std::endl;
            exit(1);
        }


            rd_kafka_topic_conf_t* topic_conf = rd_kafka_topic_conf_new();

            
            rd_kafka_topic_conf_set(topic_conf, "retention.ms", "86400000", nullptr, 0);

            
            rd_kafka_topic_conf_set(topic_conf, "retention.bytes", "1048576", nullptr, 0);

            kafka_topic = rd_kafka_topic_new(kafka_producer, "http_requests", topic_conf);



    }

    void InitializeKafkaConsumer() {
        rd_kafka_conf_t* conf = rd_kafka_conf_new();
        rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092", nullptr, 0);
        rd_kafka_conf_set(conf, "group.id", "http_requests_group", nullptr, 0);

        kafka_consumer = rd_kafka_new(RD_KAFKA_CONSUMER, conf, nullptr, 0);

        if (!kafka_consumer) {
            std::cerr << "Failed to create Kafka consumer" << std::endl;
            exit(1);
        }

        if (rd_kafka_brokers_add(kafka_consumer, "localhost:9092") == 0) {
            std::cerr << "Failed to add brokers to Kafka consumer" << std::endl;
            exit(1);
        }

        rd_kafka_topic_partition_list_t* topics = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(topics, "http_requests", RD_KAFKA_PARTITION_UA);

        if (rd_kafka_subscribe(kafka_consumer, topics) != RD_KAFKA_RESP_ERR_NO_ERROR) {
            std::cerr << "Failed to subscribe to Kafka topic" << std::endl;
            exit(1);
        }

        rd_kafka_topic_partition_list_destroy(topics);
    }

    void handle_post(http_request message) {
        message.extract_json().then([=](pplx::task<json::value> task) {
            try {
                const json::value& request_body = task.get();
                json::value response;
                response[U("message")] = json::value::string(U("POST request handled"));
                response[U("data")] = request_body;

                message.reply(status_codes::Created, response);

                std::string request_body_str = request_body.serialize();

                
                SendToKafka(request_body_str, "http_requests");

            } catch (const http_exception& e) {
                std::wcerr << L"Error extracting JSON: " << e.what() << std::endl;
                message.reply(status_codes::BadRequest, U("Invalid JSON in request body"));
            } catch (const std::exception& e) {
                std::wcerr << L"Error handling POST request: " << e.what() << std::endl;
                message.reply(status_codes::InternalError, U("An error occurred while processing the request"));
            }
        });
    }

void handle_get(http_request message) {
    while (true) {
        rd_kafka_message_t* kafka_message = rd_kafka_consumer_poll(kafka_consumer, 1000);

        if (kafka_message) {
            std::string kafka_message_payload = static_cast<const char*>(kafka_message->payload);

            json::value response;
            response[U("message")] = json::value::string(U("GET request handled"));
            response[U("kafka_message")] = json::value::string(U(kafka_message_payload));

            std::cout << "Received Kafka message: " << kafka_message_payload << std::endl;

            message.reply(status_codes::OK, response);

            rd_kafka_message_destroy(kafka_message);
        } else {
            std::cout << "No Kafka message available" << std::endl;

            json::value response;
            response[U("message")] = json::value::string(U("GET request handled"));
            response[U("kafka_message")] = json::value::string(U("No Kafka message available"));

            message.reply(status_codes::OK, response);
        }
    }
}


    void start() {
        try {
            listener.open().wait();

            utility::string_t url = U("http://localhost:8080");
            std::cout << "Listening on: " << url << std::endl;
        } catch (const std::exception& e) {
            std::wcerr << L"Error opening listener: " << e.what() << std::endl;
        }

        std::cout << "Press Enter to exit." << std::endl;
        std::string line;
        std::getline(std::cin, line);
    }

    void SendToKafka(const std::string& message, const std::string& topic) {
        if (!kafka_producer) {
            std::cerr << "Kafka producer not initialized" << std::endl;
            return;
        }

        rd_kafka_produce(
            kafka_topic,
            RD_KAFKA_PARTITION_UA,
            RD_KAFKA_MSG_F_COPY,
            const_cast<void*>(static_cast<const void*>(message.c_str())),
            message.size(),
            nullptr,
            0,
            nullptr
        );
    }
};

int main() {
    utility::string_t url = U("http://localhost:8080");
    RestServer server(url);

    server.start();

    return 0;
}
