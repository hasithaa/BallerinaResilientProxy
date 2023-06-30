import ballerina/http;
import ballerina/log;

service / on new http:Listener(8080) {

    resource function get user/[string id]() returns json {
        self.count += 1;
        string msg = "User " + id + " Found";
        log:printInfo(msg);
        return {msg};
    }

    private int count = 0;
    resource function post user(@http:Payload json payload) returns json|error {
        self.count += 1;
        if self.count % 3 == 0 {
            return error("Mock Server Error");
        }
        string msg = "User Added";
        log:printInfo(msg);
        return {msg, payload};
    }

    resource function put user/[string id](@http:Payload json payload) returns json|error {
        self.count += 1;
        if self.count % 4 == 0 {
            return error("Mock Server Error");
        }
        string msg = "User " + id + " Updated";
        log:printInfo(msg);
        return {msg, payload};
    }

    resource function delete user/[string id]() returns json|error {
        self.count += 1;
        if self.count % 2 == 0 {
            return error("Mock Server Error");
        }
        self.count = self.count + 1;
        string msg = "User " + id + " Deleted";
        log:printInfo(msg);
        return {msg};
    }

}
