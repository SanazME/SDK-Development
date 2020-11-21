import boto3
import constants


def create_reservation_table_wrapper():
    # Name of table
    table_name = constants.RESERVATION_TABLE_NAME

    # Attributes for partition keys and sort key
    customer_id_attr_name = 'CustomerId'
    city_attr_name = 'City'
    date_attr_name = 'Date'
    
    # Name of the global secondaty index GSI
    gsi_name = 'ReservationByCityDate'
    
    # Create a DynamoDB table and global secondary index
    create_reservation_table(
        table_name,
        gsi_name,
        customer_id_attr_name,
        city_attr_name,
        date_attr_name)


def create_reservation_table(
    table_name,
    gsi_name,
    customer_id_attr_name,
    city_attr_name,
    date_attr_name):

    key_schema =  [
            {
                'AttributeName': customer_id_attr_name,
                'KeyType': 'HASH'
            }
        ]

    attribute_definitions = [
        {
            'AttributeName': customer_id_attr_name,
            'AttributeType': 'S'
        },
        {
            'AttributeName': city_attr_name,
            'AttributeType': 'S'
        },
        {
            'AttributeName': date_attr_name,
            'AttributeType': 'S'
        },
    ]

    global_secondary_indexes = [
            {
                'IndexName': gsi_name,
                'KeySchema': [
                    {
                        'AttributeName': city_attr_name,
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': date_attr_name,
                        'KeyType': 'RANGE'
                    },
                ],
                'Projection': {'ProjectionType': 'ALL'},
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            },
        ]

    db = boto3.client('dynamodb')

    try:
        # Create DynamoDB table 
        table = db.create_table(
            AttributeDefinitions=attribute_definitions,
            TableName= table_name,
            KeySchema=key_schema,
            GlobalSecondaryIndexes=global_secondary_indexes,
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )

    except Exception as err:
        print('{0} Table could not be created'.format(table_name))
        print('Error message {0}'.format(err))

    # Wait until the table is created before returning
    db.meta.client.get_waiter('table_exists').wait(TableName=table_name)


def remove_reservation_table():
    return


if __name__ == '__main__':
    print('===== Reservation table is created ======')
    remove_reservation_table()
    create_reservation_table_wrapper()
