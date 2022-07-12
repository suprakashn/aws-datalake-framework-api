"""

"""


class Response:
    def __init__(self, method, status, body, payload, message=None):
        self.method = method.upper()
        self.response_status = status
        self.response_body = body
        self.source_payload = payload
        self.response_code = None
        self.message = message if message else ''
        self.response = self._create_response()

    def _create_response(self):
        if self.method == "HEALTH":
            if self.response_status:
                self.response_code = 200
                self.response_message = "The API health is good"
            else:
                self.response_code = 400
                self.response_message = (
                    "There was an error in creating the resource as requested: "
                    + self.message
                )
        elif self.method == "CREATE":
            if self.response_status:
                self.response_code = 201
                self.response_message = "Successfully created the resource as requested"
            else:
                self.response_code = 401
                self.response_message = (
                    "There was an error in creating the resource as requested: "
                    + self.message
                )
        elif self.method == "READ":
            if self.response_status:
                self.response_code = 202
                self.response_message = "Data Successfully retrieved as requested"
            else:
                self.response_code = 402
                self.response_message = (
                    "There was an error in retrieving the data as requested: "
                    + self.message
                )
        elif self.method == "UPDATE":
            if self.response_status:
                self.response_code = 203
                self.response_message = "Data Successfully updated as requested"
            else:
                self.response_code = 403
                self.response_message = (
                    "There was an error in updating the data as requested: " + self.message
                )
        elif self.method == "DELETE":
            if self.response_status:
                self.response_code = 204
                self.response_message = "Resource Successfully deleted as requested"
            else:
                self.response_code = 404
                self.response_message = (
                    "There was an error in deleting the resource as requested: "
                    + self.message
                )
        elif not self.method:
            self.response_code = 400
            self.response_message = "Please check the method provided"

        response = {
            "responseStatus": self.response_status,
            "responseCode": self.response_code,
            "responseMessage": self.response_message,
            "responseBody": self.response_body,
            "sourcePayload": self.source_payload,
        }
        return response

    def get_response(self):
        return self.response
