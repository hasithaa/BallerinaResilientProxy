import ballerina/http;
import ballerina/log;
import ballerina/uuid;
import ballerina/time;
import ballerina/task;
import ResilientProxy.db;
import ballerina/lang.'string;
import ballerina/lang.value;

final db:Client db = check new ();
final string nodeId = uuid:createType1AsString();

listener http:Listener proxyEP = new (9090);

const X_URL = "X-Url";
const X_REPLY = "X-Reply";

service on proxyEP {

    function init() returns error? {
        _ = check db->/nodes.post([{nodeId, lastSeen: time:utcNow()[0]}]);
    }

    isolated resource function default submit(http:Request req) returns Status|http:Accepted|http:BadRequest|http:InternalServerError {
        do {
            map<string> headersMap = map from var header in req.getHeaderNames()
                select [header, check req.getHeader(header)];

            if !headersMap.hasKey(X_URL) || !headersMap.hasKey(X_REPLY) {
                string message = "Required headers not found";
                http:BadRequest badRequest = {body: {message}};
                return badRequest;
            }

            // Create Payload.
            string id = uuid:createType1AsString();
            string url = headersMap.get(X_URL);
            string replyUrl = headersMap.get(X_REPLY);
            _ = headersMap.remove(X_URL);
            _ = headersMap.remove(X_REPLY);
            byte[] headers = headersMap.toJsonString().toBytes();
            byte[] payload = check req.getBinaryPayload();
            string contentType = req.getContentType();
            string method = req.method;
            int createdAt = time:utcNow()[0];
            db:MessageInsert message = {id, url, replyUrl, state: CREATED, headers, payload, nodeId, contentType, method, createdAt};
            // Persist request.
            _ = check db->/messages.post([message]);
            http:Accepted accepted = {headers: {"x-activity": id}};
            return accepted;
        } on fail error e {
            string reference = uuid:createType1AsString();
            string message = "Error occurred while submitting the transaction";
            log:printError(message, e, reference = reference);
            http:InternalServerError err = {body: {message, reference}};
            return err;
        }
    }

    isolated resource function get message(string id) returns Status|http:NotFound {
        do {
            Status status = check db->/messages/[id];
            return status;
        } on fail error e {
            string reference = uuid:createType1AsString();
            string message = "Message ID not found";
            log:printError(message, e, reference = reference);
            return {body: {message, reference}};
        }
    }
}

public function main() returns error? {
    // Starting Scheduler.
    task:JobId jobId = check task:scheduleJobRecurByFrequency(new SendMessageJob(), 0.5, 1);
}

isolated class SendMessageJob {
    *task:Job;

    public function execute() {
        // Retrieve a message and schedule it for send.
        final db:Message? msg = scheduleMessage();
        if msg is () {
            // Nothing to send.
            return;
        }
        // Send the message.
        http:Response? sendMessageResult = sendMessage(msg);
        if sendMessageResult is () {
            // Error, return to retry.
            return;
        }
        // Send the response to the reply URL.
    }
}

function scheduleMessage() returns db:Message? {
    transaction {
        stream<db:Message, error?> messages = db->/messages();
        db:Message[] messageList = check from var message in messages
            where message.state == CREATED || message.state == SENT_FAILED
            order by message.createdAt ascending
            limit 1
            select message;
        if messageList.length() == 0 {
            _ = check commit;
            return;
        } else {
            db:Message msg = messageList[0];
            _ = check db->/messages/[msg.id].put({state: SCHEDULED, nodeId});
            _ = check commit;
            return msg;
        }
    } on fail error e {
        log:printError("Error occurred while retrieving the message from DB", e);
    }
}

function sendMessage(db:Message msg) returns http:Response? {
    do {
        http:Client ep = check new (msg.url);
        map<string> reqHeaders = check (check value:fromJsonString(check string:fromBytes(msg.headers))).cloneWithType();

        http:Request request = new ();
        reqHeaders.entries().forEach(h => request.setHeader(h[0], h[1]));
        request.setBinaryPayload(msg.payload);
        check request.setContentType(msg.contentType);
        request.method = msg.method;

        http:Response|error response = ep->execute(msg.method, ".", request);
        if response is error {
            _ = check db->/messages/[msg.id].put({state: SENT_FAILED});
            fail response;
        }
        int statusCode = response.statusCode;
        match statusCode {
            200|201|202 => {
                // Update the state of the message and response
                transaction {
                    string id = uuid:createType1AsString();
                    map<string> headersMap = map from var header in response.getHeaderNames()
                        select [header, check response.getHeader(header)];
                    byte[] headers = headersMap.toJsonString().toBytes();
                    byte[] payload = check response.getBinaryPayload();
                    string contentType = response.getContentType();
                    db:Response res = {id, headers, payload, contentType, statusCode, responseId: msg.id};
                    _ = check db->/responses.post([res]);
                    _ = check db->/messages/[msg.id].put({state: SENT});
                    check commit;
                }
            }
            _ => {
                _ = check db->/messages/[msg.id].put({state: SENT_FAILED});
                log:printError("Error occurred while sending the message to the URL", reason = "Status code mismatch", code = response.statusCode);
            }
        }
    } on fail error e {
        log:printError("Error occurred while sending the message to the URL", e, message = msg.id);
    }
}

type Status record {|
    string id;
    State state;
|};

# Description of the state of the transaction
enum State {
    CREATED,
    SCHEDULED,
    SENT,
    SENT_FAILED,
    REPLY,
    REPLY_FAILED,
    COMPLETED
}
