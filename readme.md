g++ -o simple_http_server webserver.cpp -pthread
./simple_http_server

 ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic http_requests --group http_requests_group


./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic http_requests

g++ -o myapp webserver.cpp -lcpprest -lboost_system -lssl -lcrypto -lrdkafka -pthread -L/path/to/librdkafka/lib -I/path/to/librdkafka/include


g++ -o myapp webserver.cpp -lcpprest -lboost_system -lssl -lcrypto -pthread
./myapp


./kafka-console-producer.sh --broker-list localhost:9092 --topic http_requests

./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic http_requests


// without kafka restapi

#include <iostream>
#include <cpprest/http_listener.h>
#include <cpprest/json.h>

using namespace web;
using namespace web::http;
using namespace web::http::experimental::listener;

class RestServer {
public:
    RestServer(const utility::string_t& url)
        : listener(url) {
        listener.support(methods::GET, std::bind(&RestServer::handle_get, this, std::placeholders::_1));
        listener.support(methods::POST, std::bind(&RestServer::handle_post, this, std::placeholders::_1));
    }

    void start() {
        try {
            listener.open().wait();

            utility::string_t url = U("http://localhost:8080");
            std::cout << "Listening on: " << url << std::endl;
        }
        catch (const std::exception& e) {
            std::wcerr << L"Error opening listener: " << e.what() << std::endl;
        }

        std::cout << "Press Enter to exit." << std::endl;
        std::string line;
        std::getline(std::cin, line);
    }

private:
    void handle_get(http_request message) {

        json::value response;
        response[U("message")] = json::value::string(U("GET request handled"));
        message.reply(status_codes::OK, response);
    }

 void handle_post(http_request message) {
    message.extract_json().then([=](pplx::task<json::value> task) {
        try {
            const json::value& request_body = task.get();
            json::value response;
            response[U("message")] = json::value::string(U("POST request handled"));
            message.reply(status_codes::Created, response);
        } catch (const http_exception& e) {
            std::wcerr << L"Error extracting JSON: " << e.what() << std::endl;
            message.reply(status_codes::BadRequest, U("Invalid JSON in request body"));
        } catch (const std::exception& e) {
            std::wcerr << L"Error handling POST request: " << e.what() << std::endl;
            message.reply(status_codes::InternalError, U("An error occurred while processing the request"));
        }
    });
}

    http_listener listener;
};



int main() {
    utility::string_t url = U("http://localhost:8080");
    RestServer server(url);

    server.start();

    return 0;
}







// With kafka and socket  


#include <iostream>
#include <string>
#include <boost/asio.hpp>
#include <librdkafka/rdkafka.h>


rd_kafka_t* kafka_producer = nullptr;
rd_kafka_topic_t* kafka_topic = nullptr; 


using namespace boost::asio;
io_service service;
ip::tcp::acceptor acceptor(service, ip::tcp::endpoint(ip::tcp::v4(), 8080));

std::string HandleGetRequest() {
    return "HTTP/1.1 200 OK\r\n"
           "Content-Type: application/json\r\n"
           "\r\n"
           "{ \"message\": \"GET request handled!\" }";
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

    
    kafka_topic = rd_kafka_topic_new(kafka_producer, "http_requests", nullptr);
}

void SendToKafka(const std::string& message) {
    if (!kafka_producer) {
        std::cerr << "Kafka producer not initialized" << std::endl;
        return;
    }

    if (!kafka_topic) {
        std::cerr << "Kafka topic not initialized" << std::endl;
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

std::string HandleRequest(const std::string& request) {
    std::istringstream iss(request);
    std::string method;
    iss >> method;

    if (method == "GET") {
        return HandleGetRequest();
    } else {
        return "HTTP/1.1 405 Method Not Allowed\r\n"
               "Content-Type: text/plain\r\n"
               "\r\n"
               "Method Not wrk";
    }
}

int main() {

    std::cout << "Please wait port is checking" << std::endl;
 
 for (int i = 0; i<2; i++){
        InitializeKafkaProducer();
    std::cout <<  "check no " << i << std::endl;
 }
    // InitializeKafkaProducer();

    std::cout << "COOOOOLLLLLL , Wooho Working !!!!!!" << std::endl;
    std::cout << "Server is running on port 8080" << std::endl;

    while (true) {
        ip::tcp::socket socket(service);
        acceptor.accept(socket);

        char data[1024];
        boost::system::error_code error;
        size_t len = socket.read_some(buffer(data), error);

        if (!error) {
            std::string request(data, len);
            std::string response = HandleRequest(request);

            SendToKafka(response);


            boost::system::error_code ignored_error;
            write(socket, buffer(response), ignored_error);
        }
    }

    return 0;
}





