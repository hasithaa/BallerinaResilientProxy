// Copyright (c) 2023, WSO2 LLC. (https://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/http;
import ballerina/log;
import ballerina/task;
import ballerina/time;
import ballerina/uuid;

import ballerina/lang.'string;
import ballerina/lang.value;

import ResilientProxy.db;

configurable string nodeId = ?; //uuid:createType1AsString();
configurable int:Unsigned16[] allowedResponseCodes = [200, 201, 202];
configurable int retentionPeriod = 1 * 24 * 60 * 60; //  1 day in seconds.
configurable int listenPort = 9090; // JBug. Can't use int:Unsigned16 here.

final db:Client db = check new ();

listener http:Listener httpListener = new (listenPort);

const X_URL = "X-Url";
const X_REPLY = "X-Reply";
const X_REPLY_METHOD = "X-ReplyMethod";
const X_TASK_ID = "X-TaskId";

service on httpListener {

    function init() {
        log:printDebug("Initializing Resilient Proxy.");
    }

    isolated resource function default submit(http:Request req) returns http:Accepted|http:BadRequest|http:InternalServerError {
        // TODO : Handle Authentication and Authorization.
        do {
            map<string> headersMap = map from var header in req.getHeaderNames()
                select [header, check req.getHeader(header)];

            if !headersMap.hasKey(X_URL) || !headersMap.hasKey(X_REPLY) || !headersMap.hasKey(X_REPLY_METHOD) {
                string message = string `Required headers not found:` +
                (!headersMap.hasKey(X_URL) ? string `'${X_URL}' ` : "") +
                (!headersMap.hasKey(X_REPLY) ? string `'${X_REPLY}' ` : "") +
                (!headersMap.hasKey(X_REPLY_METHOD) ? string `'${X_REPLY_METHOD}'"` : "");
                string reference = logHttpClientError(message);
                http:BadRequest badRequest = {body: {message, reference}};
                return badRequest;
            }

            // Create Payload.
            string id = uuid:createType1AsString();
            string url = headersMap.get(X_URL);
            string replyUrl = headersMap.get(X_REPLY);
            string replyMethod = headersMap.get(X_REPLY_METHOD);
            _ = headersMap.remove(X_URL);
            _ = headersMap.remove(X_REPLY);
            _ = headersMap.remove(X_REPLY_METHOD);
            byte[] headers = headersMap.toJsonString().toBytes();
            byte[] payload = check req.getBinaryPayload();
            string contentType = req.getContentType();
            string method = req.method;
            int createdAt = time:utcNow()[0];

            db:MessageInsert message = {
                id,
                url,
                replyUrl,
                replyMethod,
                state: CREATED,
                nodeId,
                createdAt,
                headers,
                payload,
                contentType,
                method
            };
            // Persist request and accept.
            _ = check db->/messages.post([message]);
            http:Accepted accepted = {headers: {X_ACTIVITY: id}};
            return accepted;
        } on fail error e {
            string message = "Error occurred while processing transaction. Retry again.";
            string reference = logHttpClientError(message, e);
            http:InternalServerError err = {body: {message, reference}};
            return err;
        }
    }

    isolated resource function get message(string id) returns Status|http:NotFound {
        do {
            Status status = check db->/messages/[id];
            return status;
        } on fail error e {
            string message = "Message ID not found";
            string reference = logHttpClientError(message, e);
            return {body: {message, reference}};
        }
    }
}

public function main() returns error? {
    // Starting Scheduler.

    task:TaskPolicy policy = {
        errorPolicy: "CONTINUE"
    };
    _ = check task:scheduleJobRecurByFrequency(new SendMessageJob(), 0.5, taskPolicy = policy);
    _ = check task:scheduleJobRecurByFrequency(new ScheduleFailedMessageJob(), 5, taskPolicy = policy);
    _ = check task:scheduleJobRecurByFrequency(new RetryFailedReplyJob(), 5, taskPolicy = policy);
    _ = check task:scheduleJobRecurByFrequency(new CleanupJob(), 10, taskPolicy = policy);
    log:printDebug("Scheduler started.");
}

isolated class SendMessageJob {
    *task:Job;

    public function execute() {
        // Retrieve a message and schedule it for send.
        final db:Message? msg = scheduleMessage();
        if msg is () {
            return;
        }
        // Send the message.
        db:Response? sendMessageResult = sendMessage(msg);
        if sendMessageResult is () {
            // Error, return to retry.
            return;
        }
        // Send the response to the reply URL.
        sendReply(msg.id, sendMessageResult, msg.replyUrl, msg.replyMethod);
    }
}

isolated class ScheduleFailedMessageJob {
    *task:Job;

    public function execute() {
        do {
            // transaction { 
            // JBug. Can't use transaction here with a persistent store. Hence using a do block.
            // Fixed in Update 7. SQL connector requires a new patch release. 
            stream<db:Message, error?> messages = db->/messages();
            check from var message in messages
                where message.state == SENT_FAILED
                order by message.createdAt ascending
                do {
                    _ = check db->/messages/[message.id].put({state: SCHEDULED, nodeId});
                };
            // _ = check commit;
        } on fail error e {
            // JBug. Why Error message is not printed on the log. 
            log:printError("[ScheduleFailedJob] Can't get messages from DB", e);
        }
    }
}

isolated class RetryFailedReplyJob {
    *task:Job;

    public function execute() {
        do {
            // transaction {
            stream<db:Message, error?> messages = db->/messages();
            db:Message[] messageList = check from var message in messages
                where message.state == REPLY_FAILED
                order by message.createdAt ascending
                limit 1
                select message;

            if messageList.length() == 0 {
                // check commit;
                return;
            } else {
                db:Message msg = messageList[0];
                stream<db:Response, error?> responses = db->/responses;
                db:Response[] responseList = check from var res in responses
                    where res.responseId == msg.id
                    select res;
                sendReply(msg.id, responseList[0], msg.replyUrl, msg.replyMethod);
                // check commit;
                return;
            }
        } on fail error e {
            log:printError("[RetryFailedJob] Can't get failed messages from DB", e);
        }
    }
}

isolated class CleanupJob {
    *task:Job;

    public function execute() {
        do {
            // transaction {
            int currentTime = time:utcNow()[0];
            stream<db:Message, error?> messages = db->/messages();
            stream<db:Response, error?> responses = db->/responses;
            check from var message in messages
                where message.state == COMPLETED && retentionPeriod < currentTime - message.createdAt
                join var response in responses on message.id equals response.responseId
                do {
                    log:printDebug("Deleting message " + message.id + " and response " + response.id);
                    // JBug. Swap the order of the following two lines. and using check doesn't trigger the on fail.
                    _ = check db->/responses/[response.id].delete();
                    _ = check db->/messages/[message.id].delete();
                };
            // _ = check commit;
        } on fail error e {
            log:printError("[CleanUp Tasks] Can't get messages from DB", e);
        }
    }
}

isolated function scheduleMessage() returns db:Message? {
    do {
        // transaction {
        stream<db:Message, error?> messages = db->/messages();
        db:Message[] messageList = check from var message in messages
            where message.state == CREATED || message.state == SCHEDULED
            order by message.createdAt ascending
            limit 1
            select message;
        if messageList.length() == 0 {
            // _ = check commit;
            return;
        } else {
            db:Message msg = messageList[0];
            _ = check db->/messages/[msg.id].put({state: SCHEDULED, nodeId});
            // _ = check commit;
            return msg;
        }
    } on fail error e {
        log:printError("[Schedule] Can't get messages from DB", e);
    }
    return;
}

isolated function sendMessage(db:Message msg) returns db:Response? {
    do {
        http:Client ep = check new (msg.url);
        http:Request request = check createHttpRequest(msg, msg.method);
        http:Response|error response = ep->execute(msg.method, "", request);
        if response is error {
            _ = check db->/messages/[msg.id].put({state: SENT_FAILED});
            fail response;
        }

        final int statusCode = response.statusCode;
        if allowedResponseCodes.some(i => i == statusCode) {
            log:printDebug("Message sent", message = msg.id);
            // Update the state and response
            do {
                // transaction {
                string id = uuid:createType1AsString();
                map<string> headersMap = {};
                // JBug. map query expression fails here. (But works in the other places) (Fixed in Update 7)
                foreach var header in response.getHeaderNames() {
                    headersMap[header] = check response.getHeader(header);
                }
                byte[] headers = headersMap.toJsonString().toBytes();
                byte[] payload = check response.getBinaryPayload();
                string contentType = response.getContentType();
                db:Response res = {id, headers, payload, contentType, statusCode, responseId: msg.id};
                _ = check db->/responses.post([res]);
                _ = check db->/messages/[msg.id].put({state: SENT});
                // check commit;
                return res;
            }
        } else {
            _ = check db->/messages/[msg.id].put({state: SENT_FAILED});
            log:printError("[Schedule] Can't send the message to the URL", reason = "Status code mismatch", code = response.statusCode, message = msg.id, payload = check response.getTextPayload());
        }
    } on fail error e {
        log:printError("[Schedule] Can't send the message to the URL", e, message = msg.id);
    }
    return;
}

isolated function sendReply(string msgId, db:Response res, string replyUrl, string replyMethod) {
    do {
        http:Client ep = check new (replyUrl);
        http:Request request = check createHttpRequest(res, replyMethod);
        request.setHeader(X_TASK_ID, msgId);
        http:Response|error response = ep->execute(replyMethod, "", request);
        if response is error {
            _ = check db->/messages/[msgId].put({state: REPLY_FAILED});
            fail response;
        }
        final int statusCode = response.statusCode;
        if allowedResponseCodes.some(i => i == statusCode) {
            log:printDebug("Message Complete", message = msgId);
            _ = check db->/messages/[msgId].put({state: COMPLETED});
        } else {
            _ = check db->/messages/[msgId].put({state: REPLY_FAILED});
            // TODO: Why we not printing the error msg here?
            log:printError("Error occurred while sending the reply to the URL", reason = "Status code mismatch", code = response.statusCode, message = msgId, payload = check response.getTextPayload());
        }
    } on fail error e {
        log:printError("Error occurred while sending the reply to the URL", e, message = msgId);
    }
}

# State of the activity.
enum State {
    CREATED,
    SCHEDULED,
    SENT,
    SENT_FAILED,
    REPLY_FAILED,
    COMPLETED
}

# Represents the activity state
type Status record {|
    # Activity ID.
    string id;
    # State of the transaction.
    State state;
|};

# Represents a network message, which is a request or a response.
type NetworkMessage record {
    # Headers of the message in serialized form.
    byte[] headers;
    # Payload of the message in serialized form.
    byte[] payload;
    # Content type of the payload.
    string contentType;
};

// type MessageWithResponse record {|
//     *db:Message;
//     db:Response response;
// |};

isolated function createHttpRequest(NetworkMessage msg, string method) returns http:Request|error {
    map<string> reqHeaders = check (check value:fromJsonString(check string:fromBytes(msg.headers))).cloneWithType();

    http:Request request = new ();
    foreach [string, string] kv in reqHeaders.entries() {
        request.setHeader(kv[0], kv[1]);
    }
    request.setBinaryPayload(msg.payload);
    check request.setContentType(msg.contentType);
    request.method = method;

    return request;
}

isolated function logHttpClientError(string message, error? err = (), *log:KeyValues keyValues) returns string {
    string reference = uuid:createType1AsString();
    keyValues["reference"] = reference;
    log:printError(message, err, (), keyValues);
    return reference;
}
