import ballerina/http;
import ballerina/log;
import ballerina/task;
import ballerina/time;

const X_TASK_ID = "X-TaskId";

service /callback on new http:Listener(8081) {

    int count = 0;
    resource function post result(http:Request res) returns error? {
        self.count += 1;
        if self.count % 3 == 0 {
            return error("Mock Error");
        }
        log:printInfo("Got Response " + check res.getHeader(X_TASK_ID));
    }
}

isolated class ClientJob {

    private final map<string> & readonly headers = {
        "X-Url": "http://localhost:8080/user",
        "X-Reply": "http://localhost:8081/callback/result",
        "X-ReplyMethod": "POST"
    };

    *task:Job;

    public function execute() {
        do {
            http:Client ep = check new ("http://localhost:9090", cache = {enabled: false});
            // JBug. HTTP Caching client does not support headers. It drops headers.
            json payload = {"name": "John Doe", time: time:utcNow()[0]};
            http:Response res = check ep->/submit.post(payload, self.headers);
            log:printInfo("Submitted task: " + check res.getHeader(X_TASK_ID));
        } on fail {
            // Ignore error.
        }
    }
}

public function main() returns error? {
    _ = check task:scheduleJobRecurByFrequency(new ClientJob(), 5);
}
