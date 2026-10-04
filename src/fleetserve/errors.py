class FleetError(Exception):
    def __init__(self, code, message='', status=400):
        super().__init__(message or code)
        self.code = code
        self.status = status

    def wire(self):
        return {'error': {'code': self.code, 'message': str(self)}}
