High-performance Async Write-Ahead Log DB

> Note: I AM BUILDING THIS PROJECT, BECAUSE I FUMBLED TO ANSWER INTERVIEW QUESTIONS ABOUT TOKIO, SO FUCK IT, I BALL!!!.

**High-level design**

![Design](mini-db-design.png)

**Network protocol**

- Request
  GET/SET/DELETE = 0x00/0x01/0x02 (1B op|4B key_length|4B value_length|key|value|)

- Response
  OK/ERR = 0x00/0x01 (1B status|4B value_length|value|)