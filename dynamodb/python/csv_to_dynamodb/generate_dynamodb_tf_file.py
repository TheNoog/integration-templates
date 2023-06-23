#!/usr/bin/env python


# Takes in a CSV file and generates a dynamodb.tf file to setup the table ready for ingestion.

import pandas as pd
import os
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)



def _collect_csv(CSV_NAME):

    DATA_DIR = "/".join(os.getcwd().split('/')[1:6]) + '/data'
    df = pd.read_csv(f"/{DATA_DIR}/{CSV_NAME}.csv")

    return df



def _build_dynamodb_tf(dataframe, CSV_NAME):

    # Builds a Terraform file to build our DynamoDB table

    DYNAMODB_TF_START = """resource "aws_dynamodb_table" "dynamodb-table" {
        name           = \"""" + CSV_NAME + """\"
        billing_mode   = "PROVISIONED"
        read_capacity  = 20
        write_capacity = 20
        hash_key       = \"""" + dataframe.columns[0] + """\"
        # range_key      = "GameTitle"
    """

    DYNAMODB_TF_END = """
        ttl {
            attribute_name = "TimeToExist"
            enabled        = false
        }

        # global_secondary_index {
        #     name               = "GameTitleIndex"
        #     hash_key           = "GameTitle"
        #     range_key          = "TopScore"
        #     write_capacity     = 10
        #     read_capacity      = 10
        #     projection_type    = "INCLUDE"
        #     non_key_attributes = ["UserId"]
        # }

        # tags = {
        #     Name        = "dynamodb-table-1"
        #     Environment = "production"
        # }
    }     
    """

    if dataframe.dtypes[0] == "int64":
        DTYPE = "N"
    else:
        DTYPE = "S"


    ATTRIBUTE = """
        attribute {
            name = \"""" + dataframe.columns[0] + """\"
            type = \"""" + DTYPE + """\"
        }    
    """


    DYNAMODB_TF = DYNAMODB_TF_START + ATTRIBUTE + DYNAMODB_TF_END

    print(DYNAMODB_TF)

    with open('dynamodb_tech_interview.tf', 'w') as output_file:
        output_file.write(DYNAMODB_TF)




if __name__ == '__main__':

    CSV_NAME="waste_recycling"

    df = _collect_csv(CSV_NAME)

    _build_dynamodb_tf(df, CSV_NAME)    