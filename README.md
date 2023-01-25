# Custom-Metrics-Emitter
This sample project show how to implement sending a custom metric events to Azure Event Hub.
In this case, we will calculate the unprocessed events (The Lag) of specific consumer group in event hub.
This metric is not available in event hub standard metrics for now.
More about custom metrics in Azure Monitor check [this article](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/metrics-custom-overview) 

## High-Level Solution Concept
![image](design/design.png)

## Pre-requisite
1. Deploy the following Azure services:
   1.  Azure Event Hub
   2.  Azure Storage
   3.  Azure Application Insights (optional)
2. Producer and Consumer sample application for Azure Event hub [code example](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-dotnet-standard-getstarted-send?tabs=passwordless%2Croles-azure-portal)
3. Decide on one of the two options for authentication:
   - Service Principal (Azure AD -App Registration)
   - User Managed Identity
- Once decided on the authentication method - the chosen identity should get the following roles:
  - `Monitoring Metrics Publisher` role for Azure Event Hub
  - `Storage Blob Data Reader` for Azure Storage

## Build and Publish
The solution can be deployed on either Azure Container App or any other Azure service which able to host container solution and has User Managed Identity support (in case this is the selected authentication method - see above).

1. Build and Publish a docker image, in the sample - we are building and publishing the image using Github action which trigger once a new release created.
2. Deploy the solution on the chosen Azure Service, in the sample - we are using Azure Container App, 
   - if the chosen authentication method is User Managed Identity - assign the identity to the service.
   - The following environemnt variables should be set:
     - `TenantId`             - { Tenant Id of Azure Subscription }
     - `SubscriptionId`          - { Azure Subscription Id }
     - `ResourceGroup`            - { Resource Group name where the Azure Event Hub deployed }
     - `Region`                   - { Region name where the Azure Event Hub deployed }
     - `EventHubNamespace`        - { Name of Azure Event Hub Namespace}
     - `EventHubName`             - { Name of Azure Event Hub }
     - `ConsumerGroup`            - { Consumer Group which need to be monitored }
     - `CheckpointAccountName`    - { Azure Storage name of checkpoint }
     - `CheckpointContainerName`  - { Container name which the consumer update }      
     - `CustomMetricInterval` (optional) - { Metric send interval in ms, default: 10000ms }
     - `ManagedIdentityClientId` (optional) - { User Managed Identity client id in case of managed identity authentication}
     - `AZURE_TENANT_ID` (optional) - {tenand Id in case of service principal authentication }
     - `AZURE_CLIENT_ID` (optional) - {spn client id in case of service principal authentication }
     - `AZURE_CLIENT_SECRET` (optional) - { spn client secret in case of principal authentication }
     - `APPLICATIONINSIGHTS_CONNECTION_STRING` (optional) - Azure Application Insights connection string

Example of running the docker image locally:

`docker run -d -e EventHubNamespace="{EventHubNamespace}" -e Region="{Region}" -e SubscriptionId="{SubscriptionId}" -e ResourceGroup="{ResourceGroup}" -e TenantId="{TenantId}"  -e EventHubName="{EventHubName}" -e ConsumerGroup="{ConsumerGroup}" -e CheckpointAccountName="{CheckpointAccountName}" -e CheckpointContainerName="{CheckpointContainerName}" -e CustomMetricInterval="{CustomMetricInterval}" -e ManagedIdentityClientId="{ManagedIdentityClientId}" -e APPLICATIONINSIGHTS_CONNECTION_STRING="{APPLICATIONINSIGHTS_CONNECTION_STRING}" -e AZURE_TENANT_ID={AZURE_TENANT_ID} -e AZURE_CLIENT_ID={AZURE_CLIENT_ID} -e AZURE_CLIENT_SECRET={AZURE_CLIENT_SECRET}  <dockerimagename>`

## Custom Metric
Example of json schema which send a custom metric can be found [here](test/custom1.json)
As the schema also include the partition number as one of the dimensions - we can have a view of unprocessed events per partition:
![image](design/view.png)
