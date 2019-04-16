USE Test

DROP DATABASE ServiceBrokerTest2
CREATE DATABASE ServiceBrokerTest2
ALTER DATABASE ServiceBrokerTest2
SET ENABLE_BROKER;

USE ServiceBrokerTest2
-- For Request
CREATE MESSAGE TYPE
[//SBTest/SBSample/RequestMessage]
VALIDATION=WELL_FORMED_XML;
-- For Reply
CREATE MESSAGE TYPE
[//SBTest/SBSample/ReplyMessage]
VALIDATION=WELL_FORMED_XML;  

--Create Contract for the Conversation 
USE ServiceBrokerTest2
CREATE CONTRACT [//SBTest/SBSample/SBContract]
(
[//SBTest/SBSample/RequestMessage]
SENT BY INITIATOR 
,[//SBTest/SBSample/ReplyMessage]
SENT BY TARGET 
);

USE ServiceBrokerTest2
--Create Queue for the Initiator
CREATE QUEUE SBInitiatorQueue; 
--Create Queue for the Target
CREATE QUEUE SBTargetQueue; 

--Create Service for the Target and the Initiator.
USE ServiceBrokerTest2 
--Create Service for the Initiator.
CREATE SERVICE [//SBTest/SBSample/SBInitiatorService]
ON QUEUE SBInitiatorQueue; 
--Create Service for the Target.
CREATE SERVICE [//SBTest/SBSample/SBTargetService] 
ON QUEUE SBTargetQueue
([//SBTest/SBSample/SBContract]); 
--Sending a Request Message to the Target

USE ServiceBrokerTest2 
DECLARE @InitDlgHandle UNIQUEIDENTIFIER
DECLARE @RequestMessage VARCHAR(1000) 

BEGIN TRAN 
--Determine the Initiator Service, Target Service and the Contract 
BEGIN DIALOG @InitDlgHandle
FROM SERVICE
[//SBTest/SBSample/SBInitiatorService]
TO SERVICE
'//SBTest/SBSample/SBTargetService'
ON CONTRACT
[//SBTest/SBSample/SBContract]
WITH ENCRYPTION=OFF; 
--Prepare the Message
SELECT @RequestMessage = N'<RequestMessage> wiadomosc od nadawcy </RequestMessage>'; 
--Send the Message
SEND ON CONVERSATION @InitDlgHandle 
MESSAGE TYPE
[//SBTest/SBSample/RequestMessage]
(@RequestMessage);
SELECT @RequestMessage AS SentRequestMessage;
COMMIT TRAN 

--Receiving a Message and sending a Reply from the Target 
USE ServiceBrokerTest2 
DECLARE @TargetDlgHandle UNIQUEIDENTIFIER
DECLARE @ReplyMessage VARCHAR(1000)
DECLARE @ReplyMessageName Sysname 
BEGIN TRAN; 
--Receive message from Initiator
RECEIVE TOP(1)
@TargetDlgHandle=Conversation_Handle
,@ReplyMessage=Message_Body
,@ReplyMessageName=Message_Type_Name
FROM SBTargetQueue; 
SELECT @ReplyMessage AS ReceivedRequestMessage; 
-- Confirm and Send a reply
IF @ReplyMessageName=N'//SBTest/SBSample/RequestMessage'
BEGIN
DECLARE @RplyMsg VARCHAR(1000)
SELECT @RplyMsg =N'<RplyMsg> wiadomosc zwrotna do odbiorny</RplyMsg>'; 
SEND ON CONVERSATION @TargetDlgHandle
MESSAGE TYPE
[//SBTest/SBSample/ReplyMessage]
(@RplyMsg);
END CONVERSATION @TargetDlgHandle;
END 
SELECT @RplyMsg AS SentReplyMessage; 
COMMIT TRAN;

--Receiving Reply Message from the Target.
USE ServiceBrokerTest2 
DECLARE @InitiatorReplyDlgHandle UNIQUEIDENTIFIER
DECLARE @ReplyReceivedMessage VARCHAR(1000) 
BEGIN TRAN; 
RECEIVE TOP(1)
@InitiatorReplyDlgHandle=Conversation_Handle
,@ReplyReceivedMessage=Message_Body
FROM SBInitiatorQueue; 
END CONVERSATION @InitiatorReplyDlgHandle; 
SELECT @ReplyReceivedMessage AS ReceivedRepliedMessage; 
COMMIT TRAN; 


--Checking the usage of the Messages, Contracts and Queues using System views.
USE ServiceBrokerTest2 
SELECT * FROM sys.service_contract_message_usages 
SELECT * FROM sys.service_contract_usages
SELECT * FROM sys.service_queue_usages 