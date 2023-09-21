#include <iostream>
#include <cpprest/http_listener.h>
#include <cpprest/json.h>
#include <librdkafka/rdkafka.h>
#include <deque>
#include <mutex>

using namespace web;
using namespace web::http;
using namespace web::http::experimental::listener;

rd_kafka_t* kafka_producer = nullptr;
rd_kafka_topic_t* kafka_topic = nullptr;
rd_kafka_t* kafka_consumer = nullptr;
std::deque<std::string> kafka_messages; 
std::mutex kafka_messages_mutex; 

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

        rd_kafka_conf_set(conf, "auto.offset.reset", "latest", nullptr, 0);

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
                // std::cout<<"from post:::::" <<request_body_str<<std::endl;

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
        json::value response;
        response[U("message")] = json::value::string(U("GET request handled"));

        {
            std::lock_guard<std::mutex> lock(kafka_messages_mutex); 

           
            json::value kafka_messages_array;
            int i = 0;
            for (const auto& kafka_message : kafka_messages) {
                kafka_messages_array[i++] = json::value::string(U(kafka_message));
            }
            response[U("kafka_messages")] = kafka_messages_array;
            // std::cout<<"From get:::::::"<<kafka_messages_array<<std::endl;
        } 

        
        message.headers().set_cache_control(U("no-store"));

        message.reply(status_codes::OK, response);
    }

    void start() {
        try {
            listener.open().wait();

            utility::string_t url = U("http://localhost:8082");
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

        {
            std::lock_guard<std::mutex> lock(kafka_messages_mutex); 
            kafka_messages.push_back(message); 
        } 
    }
};

int main() {
    utility::string_t url = U("http://localhost:8082");
    RestServer server(url);

    server.start();

    return 0;
}

// kafka-topics.sh --create --topic http_requests --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --config retention.ms=-1
