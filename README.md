# Ballerina Resilient Proxy

A Ballerina proxy endpoint with reliable delivery

## How to run

1. Generate DataStore.
   * Navigate to `Proxy` folder and run the following command.
```
bal persist generate
```
2. Run Proxy.
```
bal run
```
3. Run the backend service.
   * Navigate to Clients folder and run the following command.
```
bal run User_Service.bal
```
4. Run the client.
   * Navigate to Clients folder and run the following command.
```
bal run User_Client.bal
```