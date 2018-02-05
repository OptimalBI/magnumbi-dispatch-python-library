class Job:
    job_id = str
    start_datetime = str
    data = object

    def __init__(self, job_id, data, start_datetime):
        self.job_id = job_id
        self.data = data
        self.start_datetime = start_datetime
        pass

    def __str__(self):
        return "job_id={},data={}".format(self.job_id, self.data);
