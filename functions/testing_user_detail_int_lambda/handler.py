import os
import json
import requests
import uuid
import time
import random
import datetime
from http import HTTPStatus
from utils.logger import log
from utils.db_connector import commit_sql, fetch_sql
from utils.sql_query import sql_generate_insert_query, sql_generate_update_query
from utils.invoke_lambda import invoke_lambda
from utils.response_headers import add_response_headers
from utils.utils import check_missing_params
from utils.utils import handle_response
from utils.utils import get_body

CURRENT_STAGE = os.environ.get('CURRENT_STAGE', 'dev')

user_data_possible_key = {
    "lname",
    "fname",
    "user_id",
    "loan_id",
    "application_num"
}
user_bank_data_possible_key = {
    "loan_tenure",
    "xyz",
    "requested_tenure",
    "requested_amt"
}
loan_data_possible_key = {
    "loan_type",
    "loan_purpose",
    "loan_interest_rate",
    "loan_processing_fees_amt",
    "loan_status",
    "loan_acc_num",
    "lender_id",
    "branch_id",
    "sub_status"
}

user_loan_insert_data_possible_key = {
    "loan_id", "user_id", "loan_type", "loan_purpose", "requested_amt", 
    "requested_tenure", "payment_frequency", "lender_approved_amt", 
    "total_interest", "disbursed_amt", "loan_interest_rate", 
    "loan_processing_fees_amt", "loan_processing_fees_percentage",
    "loan_installment_amt", "loan_tenure", "num_installment_recived",
    "amt_installment_recived", "loan_start_date", "loan_end_date",
    "loan_created_at", "loan_updated_at", "loan_status", "total_amt_to_pay",
    "description", "agent_id", "loan_acc_num", "lender_approved_date",
    "disbursed_date", "last_cleared_installment_date", "last_installment_paid_date",
    "last_installment_paid_amt", "last_cleared_installment_num",
    "annual_interest_rate", "loan_closed_date", "loan_processing_fees_gst_percentage",
    "loan_processing_fees_gst_amt", "old_loan_remaining_amt", "old_loan_acc_num",
    "rejection_reason", "insurance_amt", "insurance_gst_amt", "lender_id",
    "stamp_paper_fee_amt", "total_pre_disbursal_charges", "lock_in_tenure",
    "lock_in_breaking_percentage", "foreclousure_percentage", "apr",
    "overdue_interest_percentage", "lender_account_number", "lender_ifsc",
    "lender_cif", "branch_id", "sub_status"
}


def fetch_user_details(args):
    """
    Fetch user_details details either through Lambda or database
    Returns:
        tuple: (error, data) where error is None if successful
    """
    
    # Fetch details from database
    try:
        query = f"""SELECT * FROM loan_{CURRENT_STAGE} WHERE loan_{CURRENT_STAGE}.user_id = %s"""
        params = tuple([args.get(param) for param in ["user_id"]])
        error, data = fetch_sql(query, params)
        if error:
            log.error(f"Error fetching user_details details: {error}")
            return error, None
        return None, data
    except Exception as e:
        log.error(f"Exception in fetch_user_details_details: {str(e)}")
        return str(e), None
    


def fetch_loan_details(args):
    """
    Fetch loan_details details either through Lambda or database
    Returns:
        tuple: (error, data) where error is None if successful
    """
    
    # Fetch details from Lambda
    try:
        payload = {
            "user_id": args.get("user_id"),
            "loan_id": args.get("loan_id")
            
        }
        data = invoke_lambda(f"los-{CURRENT_STAGE}-get_loan_details", payload, "Event")
        return None, data
    except Exception as e:
        log.error(f"Exception in fetch_loan_details_details: {str(e)}")
        return str(e), None
    


def update_user_bank_details(body):
    """
    Update user_bank_details into database
    Body: Must be a dict and have two dict name-> update_data and where_data e.g body = {'update_data':{key:val},'where_data':{key:val}}
    Returns:
        error: False if successful, error message if failed
    """
    if not body:
        return "Empty body provided"
    try:
        if body.get('update_data', None) is None:
            return f"No data to update in body so could not generate SQL update query"
        keys, data = sql_generate_update_query(user_bank_data_possible_key , body.get('update_data'))
    except Exception as e:
        log.error(f"Error generating SQL query: {str(e)}")
        return f"Failed to generate SQL query: {str(e)}"

    sql_query = f"""
            UPDATE
                loan_{CURRENT_STAGE}
            SET
                {keys} 
            WHERE 
                user_id = %s AND loan_id = %s
            """
    try:
        for value in body.get('where_data').values():
            if value not in data:
                data.append(value)
        error = commit_sql(sql_query, data)
        if error:
            log.error(f"Error updating user_bank_details details: {error}")
        return error
    except Exception as e:
        log.error(f"Unexpected error updating user_bank_details: {str(e)}")
        return f"Unexpected error: {str(e)}"

def insert_user_detail(body):
    """
    Insert user_detail into database
    Returns:
        error: False if successful, error message if failed
    """
    if not body:
        return "Empty body provided"
    try:
        keys, value, data = sql_generate_insert_query(user_loan_insert_data_possible_key, body)
    except Exception as e:
        log.error(f"Error generating SQL query: {str(e)}")
        return f"Failed to generate SQL query: {str(e)}"
    sql_query = f"""
            INSERT INTO
            loan_{CURRENT_STAGE} {keys}
            VALUES
              {value};
            """
    try:
        error = commit_sql(sql_query, data)
        if error:
            log.error(f"Error inserting user_detail details: {error}")
        return error
    except Exception as e:
        log.error(f"Unexpected error updating user_detail: {str(e)}")
        return f"Unexpected error: {str(e)}"
    

def get_loans_details(body):
    """
    TODO: Implement get_loans_details logic
    
    Args:
        body: The request body containing function parameters
        
    Returns:
        tuple: Response containing error and data 
        
    Raises:
        Exception: If any error occurs during execution
    """
    try:
        # TODO: Implement business logic here
        data = {
            "loan_details": {
                "user_id": body.get("user_id"),
                "loan_id": body.get("loan_id"),
                "requested_tenure": body.get("requested_tenure")
            }
        }
        return None, data
        
    except Exception as e:
        return str(e), None

def get_bank_details(body):
    """
    TODO: Implement get_bank_details logic
    
    Args:
        body: The request body containing function parameters
        
    Returns:
        tuple: Response containing error and data 
        
    Raises:
        Exception: If any error occurs during execution
    """
    try:
        # TODO: Implement business logic here
        data = {
            "bank_details": {
                "user_id": body.get("user_id"),
                "loan_id": body.get("loan_id")
            }
        }
        return None, data
        
    except Exception as e:
        return str(e), None

def testing_user_detail_int_lambda(event, context):
    """This Lambda function is used to handle the request and response logic for testing_user_details_int_lambda"""
    try:
        # Initialize request_body
        body = get_body(event)
        REQ_KEYS = ['user_id', 'loan_id', 'requested_tenure']  
        #Check for missing parameters
        missing_params = check_missing_params(body, REQ_KEYS)
        if missing_params:
            response_body = {
                "statusCode": 400,
                "status": "failure",
                "message": f"Missing required parameters: {missing_params}",
            }
            log.error(f"Missing required parameters: {missing_params}")
            return response_body
        log.info(f"Event body: {body}")
        # Fetch operations, Pass Function arguments in fetch functions
        fetch_payload = {'user_id':body['user_id'],'loan_id': body['loan_id']}
        error, user_details_data = fetch_user_details(fetch_payload)
        if error:
            log.error(f"Error fetching user_details details: {error}")
            return handle_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                status='failure',
                message=f"Error fetching user_details details",
                error=str(error)
            )
        if not user_details_data:
            return handle_response(
                HTTPStatus.NOT_FOUND,
                status='failure',
                message=f"No user_details details found"
            )
        
        error, loan_details_data = fetch_loan_details(fetch_payload)
        if error:
            log.error(f"Error fetching loan_details details: {error}")
            return handle_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                status='failure',
                message=f"Error fetching loan_details details",
                error=str(error)
            )
        if not loan_details_data:
            return handle_response(
                HTTPStatus.NOT_FOUND,
                status='failure',
                message=f"No loan_details details found"
            )
        
        
        # Business logic operations, Pass the body argument as required
        
        err, loan_data = get_loans_details(body)
        if err:
            log.error(f"Error in get_loans_details: {err}")
            return handle_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                status='failure',
                message=str(err)
            )
        
        err, bank_data = get_bank_details(body)
        if err:
            log.error(f"Error in get_bank_details: {err}")
            return handle_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                status='failure',
                message=str(err)
            )
        
        # Update operations, Pass the argument as required in update functions in format, argument_name = {'update_data':{key:val},'where_data':{key:val}}
        # Update operations
        update_data = {
            'update_data': {'loan_tenure': random.randint(50, 100)},
            'where_data':{
                'user_id':body['user_id'],
                'loan_id':body['loan_id']
                }
        }
        error = update_user_bank_details(update_data)
        if error:
            log.error(f"Error updating user_bank_details details: {error}")
            return handle_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                status='failure',
                message=f"Error updating user_bank_details details",
                error=str(error)
            )
        combined_data = {
            "loans_data": loan_data,
            "bank_data": bank_data,
            "user_details": user_details_data,
            "loan_details": loan_details_data
        }
        
        # Insert operations, Pass the argument as required in update functions
        loan_details = {
            'loan_id': str(uuid.uuid4()),
            'user_id': str(uuid.uuid4()),
            'loan_type': None,
            'loan_purpose': 'businesses ',
            'requested_amt': 30000,
            'requested_tenure': 100,
            'payment_frequency': 'daily',
            'lender_approved_amt': 30000,
            'total_interest': 3000,
            'disbursed_amt': None,
            'loan_interest_rate': None,
            'loan_processing_fees_amt': None,
            'loan_processing_fees_percentage': 0.0,
            'loan_installment_amt': 330,
            'loan_tenure': f"{random.randint(50, 100)}",
            'num_installment_recived': None,
            'amt_installment_recived': None,
            'loan_start_date': None,
            'loan_end_date': None,
            'loan_created_at': datetime.datetime(2023, 5, 19, 12, 51, 9),
            'loan_updated_at': datetime.datetime(2023, 5, 20, 17, 26, 49),
            'loan_status': 'Rejected',
            'total_amt_to_pay': 33000,
            'description': None,
            'agent_id': 'f0dd80b8-4882-4252-9c3e-ad3bbff4c14d',
            'loan_acc_num': f"{random.randint(1000000, 9999999)}",
            'lender_approved_date': datetime.datetime(2023, 5, 20, 17, 26, 49),
            'disbursed_date': None,
            'last_cleared_installment_date': None,
            'last_installment_paid_date': None,
            'last_installment_paid_amt': None,
            'last_cleared_installment_num': 0,
            'annual_interest_rate': None,
            'loan_closed_date': None,
            'loan_processing_fees_gst_percentage': None,
            'loan_processing_fees_gst_amt': None,
            'old_loan_remaining_amt': None,
            'old_loan_acc_num': None,
            'rejection_reason': None,
            'insurance_amt': None,
            'insurance_gst_amt': None,
            'lender_id': 'MARG',
            'stamp_paper_fee_amt': None,
            'total_pre_disbursal_charges': None,
            'lock_in_tenure': None,
            'lock_in_breaking_percentage': None,
            'foreclousure_percentage': None,
            'apr': None,
            'overdue_interest_percentage': None,
            'lender_account_number': None,
            'lender_ifsc': None,
            'lender_cif': None,
            'branch_id': None,
            'sub_status': None
        }
        
        error = insert_user_detail(loan_details)
        if error:
            log.error(f"Error inserting user_detail details: {error}")
            return handle_response(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                status='failure',
                message=f"Error inserting user_detail details",
                error=str(error)
            )
        
        # Return success response, add your data 
        return handle_response(
            HTTPStatus.OK,
            status='success',
            message='Operation completed successfully',
            response=combined_data
        )

    except Exception as e:
        log.error(f"Error in testing_user_details_int_lambda function: {str(e)}")
        return handle_response(
            HTTPStatus.INTERNAL_SERVER_ERROR,
            status='failure',
            message=f"An error occurred in the testing_user_details_int_lambda",
            error=str(e)
        )

