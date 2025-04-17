from flask import Blueprint, request, jsonify, Response, stream_with_context
from app.services.report_service import ReportService
from datetime import datetime, time
import logging
import io
import gc
import ctypes
import time as time_module  # To avoid conflict with datetime.time
from app.authentication.AccessTokenValidator import AccessTokenValidator
from constants import X_AUTHENTICATED_USER_TOKEN, IS_VALIDATION_ENABLED, REQUIRED_COLUMNS_FOR_ENROLLMENTS

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

report_controller = Blueprint('report_controller', __name__)

@report_controller.route('/report/org/<org_id>', methods=['POST'])
def get_report(org_id):
    start_timer = time_module.time()
    try:
        logger.info(f"Received request to generate report for org_id={org_id}")
        if IS_VALIDATION_ENABLED.lower() == 'true':
            # Extract and validate user token
            user_token = request.headers.get(X_AUTHENTICATED_USER_TOKEN)
            if not user_token:
                logger.error("Missing 'x-authenticated-user-token' in headers.")
                return jsonify({'error': 'Authentication token is required.'}), 401
            
            user_org_id = AccessTokenValidator.verify_user_token_get_org(user_token, True)
            if not user_org_id:
                logger.error("Invalid or expired authentication token.")
                return jsonify({'error': 'Invalid or expired authentication token.'}), 401

            logger.info(f"Authenticated user with user_org_id={user_org_id}")
            if user_org_id != org_id:
                logger.error(f"User does not have access to organization ID {org_id}.")
                return jsonify({'error': f'Access denied for the specified organization ID {org_id}.'}), 403

        # Parse and validate date range
        data = request.get_json()
        if not data or 'start_date' not in data or 'end_date' not in data:
            raise KeyError("Missing 'start_date' or 'end_date' in request body.")

        start_date = datetime.strptime(data['start_date'], '%Y-%m-%d')
        end_date = datetime.strptime(data['end_date'], '%Y-%m-%d')

        start_date = datetime.combine(start_date.date(), time.min)  # 00:00:00
        end_date = datetime.combine(end_date.date(), time.max)      # 23:59:59.999999

        logger.info(f"Generating report for org_id={org_id} from {start_date} to {end_date}")
         #Validate date range
        # if (end_date - start_date).days > 365:
        #     logger.warning(f"Date range exceeds 1 year: start_date={start_date}, end_date={end_date}")
        #     return jsonify({'error': 'Date range cannot exceed 1 year'}), 400

        try:
            csv_data = ReportService.get_total_learning_hours_csv_stream(
                start_date, end_date, org_id, required_columns=REQUIRED_COLUMNS_FOR_ENROLLMENTS
            )

            if not csv_data:
                logger.warning(f"No data found for org_id={org_id} within given date range.")
                return jsonify({'error': 'No data found for the given organization ID.'}), 404

        except Exception as e:
            error_message = str(e)
            logger.error(f"Error generating CSV stream for org_id={org_id}: {error_message}")
            return jsonify({'error': 'Failed to generate the report due to an internal error.', 'details': error_message}), 500

        time_taken = round(time_module.time() - start_timer, 2)
        logger.info(f"Report generated successfully for org_id={org_id} in {time_taken} seconds")
        if csv_data:
            with open("total_learning_hours.csv", "w") as f:
                for line in csv_data:
                    f.write(line)
        response = Response(
            stream_with_context(csv_data),
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="report_{org_id}.csv"'
            }
        )

        # Explicitly trigger garbage collection to free up memory
        del csv_data
        gc.collect()

        return response

    except KeyError as e:
        error_message = str(e)
        logger.error(f"Missing required fields in request: {error_message}")
        return jsonify({'error': 'Invalid input. Please provide start_date and end_date.', 'details': error_message}), 400

    except ValueError as e:
        error_message = str(e)
        logger.error(f"Invalid date format in request: {error_message}")
        return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD.', 'details': error_message}), 400

    except FileNotFoundError as e:
        error_message = str(e)
        logger.error(f"File not found during report generation: {error_message}")
        return jsonify({'error': 'Report file could not be generated.', 'details': error_message}), 500

    except Exception as e:
        error_message = str(e)
        logger.exception(f"Unexpected error occurred: {error_message}")
        return jsonify({'error': 'An unexpected error occurred. Please try again later.', 'details': error_message}), 500
    finally: 
        gc.collect()
        try:
            logger.info("inside malloc_trim:")
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except Exception as e:
            logger.exception("malloc_trim failed:", e)
