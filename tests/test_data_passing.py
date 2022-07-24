import os
import json
import pytest


from main import EventIndexer
from tests.rabbitutils import RabbitPublisher


@pytest.fixture
def rabbitmq_config():
    return {
        "host": os.environ["RABBIT_HOST_URL"],
        "port": int(os.environ["RABBIT_HOST_PORT"]),
        "exchange": os.environ["RABBIT_EXCHANGE"],
        "routing_key": os.environ["RABBIT_ROUTING_KEY"],
        "queue_name": os.environ["RABBIT_QUEUE_NAME"],
        "user": os.environ["RABBIT_USER"],
        "password": os.environ["RABBIT_PASSWORD"],
    }


@pytest.fixture
def mongo_connection_string():
    return os.environ["MONGO_DB_CONNECTION_STRING"]


@pytest.mark.parametrize(
    "sent_message",
    [
        {
            "event_name": "Transfer",
            "filter_arguments": {"fromBlock": "latest"},
            "event_data": {
                "args": {
                    "from": "0x7b88944595a396af405225411c6D8F3Ad1285f53",
                    "to": "0x3cD751E6b0078Be393132286c442345e5DC49699",
                    "value": 43815937800485491278,
                },
                "event": "Transfer",
                "logIndex": 318,
                "transactionIndex": 188,
                "transactionHash": "0xc34e2eb0259d32458408663537419fde07a3fc95cfcf775c93565f7c343403a6",
                "address": "0x3845badAde8e6dFF049820680d1F14bD3903a5d0",
                "blockHash": "0x1013aa058d59d32ba4814f14485a5996f9bd25bd1ee034277ec0f64e03f7f8d5",
                "blockNumber": 15201691,
            },
        },
        {
            "event_name": "OrderFulfilled",
            "filter_arguments": {"fromBlock": "latest"},
            "event_data": {
                "args": {
                    "offerer": "0x0d707aAB8A4f293CD901669760F79c4658b84776",
                    "zone": "0x004C00500000aD104D7DBd00e3ae0A5C00560C00",
                    "orderHash": "0xe16c30a9fe2a0ff164ec65ec0143903f8e7777ce69f05fad0596ef4e1dac87fa",
                    "recipient": "0x87e485cB631d7616256076A9Cb64fFf5c7Baa468",
                    "offer": [
                        [2, "0x4591c791790f352685a29111eca67Abdc878863E", 7849, 1]
                    ],
                    "consideration": [
                        [
                            0,
                            "0x0000000000000000000000000000000000000000",
                            0,
                            43380000000000000,
                            "0x0d707aAB8A4f293CD901669760F79c4658b84776",
                        ],
                        [
                            0,
                            "0x0000000000000000000000000000000000000000",
                            0,
                            1205000000000000,
                            "0x8De9C5A032463C561423387a9648c5C7BCC5BC90",
                        ],
                        [
                            0,
                            "0x0000000000000000000000000000000000000000",
                            0,
                            3615000000000000,
                            "0x47299cBC829b027E409081e0013A2f697d2129De",
                        ],
                    ],
                },
                "event": "OrderFulfilled",
                "logIndex": 584,
                "transactionIndex": 305,
                "transactionHash": "0x885b7d5746b52eeb3fc1dfd792b7bcadb7d142a793a39a42d88c057ad06d9072",
                "address": "0x00000000006c3852cbEf3e08E8dF289169EdE581",
                "blockHash": "0x1013aa058d59d32ba4814f14485a5996f9bd25bd1ee034277ec0f64e03f7f8d5",
                "blockNumber": 15201691,
            },
        },
    ],
)
def test_on_message_callback(
    mocker, rabbitmq_config, mongo_connection_string, sent_message
):
    test_publisher = RabbitPublisher(rabbitmq_config)

    mock_insert_event = mocker.MagicMock(name="insert_event")
    mocker.patch("persistence.StorageSystem.insert_event", new=mock_insert_event)

    event_indexer = EventIndexer(rabbitmq_config, mongo_connection_string)
    event_indexer._is_test_run = True

    test_publisher.publish(json.dumps(sent_message), rabbitmq_config["routing_key"])

    event_indexer.process()

    assert mock_insert_event.call_count == 1
    called_event_name, called_event_data = mock_insert_event.call_args_list[0][0]
    assert called_event_name == sent_message["event_name"]
    assert called_event_data == sent_message
