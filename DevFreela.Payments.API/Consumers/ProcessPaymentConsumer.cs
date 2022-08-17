using DevFreela.Payments.API.Models;
using DevFreela.Payments.API.Services;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

namespace DevFreela.Payments.API.Consumers
{
    //Para poder ficar rodando debaixo dos panos e esperando a mensagem para poder processalas
    public class ProcessPaymentConsumer : BackgroundService
    {
        private const string queue = "Payments";
        //para criar a conexão com o rabbitMq
        private readonly IConnection _connection;

        //Responsavel por armazenar o canal a seção
        private readonly IModel _channel;

        //Quando for realizar um acesso a um serviço que esta injetado na aplicação
        //mas é um serviço que esta injetado em um ciclo de vida do tipo scoped e é criado internamente
        private readonly IServiceProvider _serviceProvider;

        public ProcessPaymentConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            var factory = new ConnectionFactory
            {
                HostName = "localhost"
            };

            _connection = factory.CreateConnection();
            _channel = _connection.CreateModel();

            //caso a mensagem nao exista ela é criada agora
            _channel.QueueDeclare(
                queue: queue,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null);
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumer = new EventingBasicConsumer(_channel);

            //definindo como a mensagem é tradada
            consumer.Received += (sender, eventArgs) =>
            {
                //obtenho o array de bytes
                var byteArray = eventArgs.Body.ToArray();

                //obtenho uma string do array de bytes
                var paymentInfoJson = Encoding.UTF8.GetString(byteArray);

                //Transformo o json em objeto
                var paymentInfo = JsonSerializer.Deserialize<PaymentInfoInputModel>(paymentInfoJson);

                ProcessPayment(paymentInfo);


                //digo para o mensagerbroken que a mensagem foi recebida
                _channel.BasicAck(eventArgs.DeliveryTag, false);
            };

            //inicializo o consumo de mensagem
            _channel.BasicConsume(queue,false, consumer);


            return Task.CompletedTask;
        }

        public void ProcessPayment(PaymentInfoInputModel paymentInfo)
        {
            using (var scope = _serviceProvider.CreateScope())
            {
                //Peguei o IpaymentService e acessei ele aqui que antes era recebido por injeção pois
                // é um metodo acessado de maneira indefinida
                var paymentService = scope.ServiceProvider.GetRequiredService<IPaymentService>();

                paymentService.Process(paymentInfo);
            }
        }
    }
}
