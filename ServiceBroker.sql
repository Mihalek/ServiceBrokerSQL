USE Test
DROP DATABASE ServiceBrokerTest
CREATE DATABASE ServiceBrokerTest
ALTER DATABASE ServiceBrokerTest 
SET ENABLE_BROKER;

USE ServiceBrokerTest
-- For Request
CREATE MESSAGE TYPE
[Zapytanie]
VALIDATION=WELL_FORMED_XML;
-- For Reply
CREATE MESSAGE TYPE
[Odpowiedz]
VALIDATION=WELL_FORMED_XML;  

--Create Contract for the Conversation 
USE ServiceBrokerTest
CREATE CONTRACT [Kontrakt]
(
[Zapytanie]
SENT BY INITIATOR 
,[Odpowiedz]
SENT BY TARGET 
);

USE ServiceBrokerTest 

CREATE QUEUE KolejkaNadawcy; 

CREATE QUEUE KolejkaOdbiorcy; 


USE ServiceBrokerTest 

CREATE SERVICE [UslugaNadawcy]
ON QUEUE KolejkaNadawcy; 
--Create Service for the Target.
CREATE SERVICE [UslugaOdbiorcy] 
ON QUEUE KolejkaOdbiorcy
([Kontrakt]); 
--Sending a Request Message to the Target

USE ServiceBrokerTest 
DECLARE @InitDlgHandle UNIQUEIDENTIFIER
DECLARE @RequestMessage VARCHAR(1000) 

BEGIN TRAN 
--Determine the Initiator Service, Target Service and the Contract 
BEGIN DIALOG @InitDlgHandle
FROM SERVICE
[UslugaNadawcy]
TO SERVICE
'UslugaOdbiorcy'
ON CONTRACT
[Kontrakt] 
WITH ENCRYPTION=OFF; 
--Prepare the Message
SELECT @RequestMessage = N'<RequestMessage> Send a Message to Target </RequestMessage>'; 
--Send the Message
SEND ON CONVERSATION @InitDlgHandle 
MESSAGE TYPE
[Zapytanie]
(@RequestMessage);
SELECT @RequestMessage AS SentRequestMessage;
COMMIT TRAN 

--Receiving a Message and sending a Reply from the Target 
USE ServiceBrokerTest 
DECLARE @TargetDlgHandle UNIQUEIDENTIFIER
DECLARE @ReplyMessage VARCHAR(1000)
DECLARE @ReplyMessageName Sysname 
BEGIN TRAN; 
--Receive message from Initiator
RECEIVE TOP(1)
@TargetDlgHandle=Conversation_Handle
,@ReplyMessage=Message_Body
,@ReplyMessageName=Message_Type_Name
FROM KolejkaOdbiorcy; 
SELECT @ReplyMessage AS ReceivedRequestMessage; 
-- Confirm and Send a reply
IF @ReplyMessageName=N'RequestMessage'
BEGIN
DECLARE @RplyMsg VARCHAR(1000)
SELECT @RplyMsg =N'<RplyMsg> Send a Message to Initiator</RplyMsg>'; 
SEND ON CONVERSATION @TargetDlgHandle
MESSAGE TYPE
[Odpowiedz]
(@RplyMsg);
END CONVERSATION @TargetDlgHandle;
END 
SELECT @RplyMsg AS SentReplyMessage; 
COMMIT TRAN;

USE ServiceBrokerTest 
DECLARE @InitiatorReplyDlgHandle UNIQUEIDENTIFIER
DECLARE @ReplyReceivedMessage VARCHAR(1000) 
BEGIN TRAN; 
RECEIVE TOP(1)
@InitiatorReplyDlgHandle=Conversation_Handle
,@ReplyReceivedMessage=Message_Body
FROM KolejkaNadawcy; 
END CONVERSATION @InitiatorReplyDlgHandle; 
SELECT @ReplyReceivedMessage AS ReceivedRepliedMessage; 
COMMIT TRAN; 

--Checking the usage of the Messages, Contracts and Queues using System views.
USE ServiceBrokerTest 
SELECT * FROM sys.service_contract_message_usages 
SELECT * FROM sys.service_contract_usages
SELECT * FROM sys.service_queue_usages 