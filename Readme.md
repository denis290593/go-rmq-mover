# Rabbitmq-mover

Example of moving messages from one queue to another in RabbitMQ.

1. cp .env.dist .env
2. edit .env
3. run command
 ```
./rabbitmq-mover -from='ecom--core--order-history-service.failed' -to='ecom--order-history-service.ecom1.historical.order.map-to-sf.handle'

./rabbitmq-mover -from='ecom--core--order-service.failed' -to='core--order-service.order.payment-refund.callback' -count=10

./rabbitmq-mover -from='ecom--core--order-history-service.failed' -count=1 ----bad work maybe need in end .handle
```

