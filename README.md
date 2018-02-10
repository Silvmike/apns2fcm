# APNs Push Id to FCM Registration token converter

This small utility uses Google's [https://iid.googleapis.com/iid/v1:batchImport](https://iid.googleapis.com/iid/v1:batchImport) service.
For more information see [this](https://developers.google.com/instance-id/reference/server#create_registration_tokens_for_apns_tokens). 

It converts existing APNs Push Ids into FCM tokens for your application.

## Usage

```
java -jar apns2fcm.jar poolSize=10 fileName=input.csv sandbox=true packageName=your.package.name serverKey=YOUR_SERVER_KEY
```

Where:

* **poolSize** is a thread pool size for batch job processing (assume, poolSize=10, it means that utility will use at most 10 connections to service),
* **fileName** is an input file name. This file should contain APNs tokens each in single line,
* **sandbox**: if it is 'true', indicates sandbox environment,
* **packageName** is your application package name,
* **serverKey** is your FCM Server API key
