
import threading
import time


##class of schedular
class schedular():
    def __init__(self,input_data,id):
        self.idempotency_id=id
        self.input_data=input_data
        self.Failure_state_retry={}
        self.Status={}
        self.job_status={}
        
        self.job_status['status']=False
        
    ## method to map country data to state data
    def get_the_countries(self,input_data):
        country_list_dict={}
        for item in input_data:
            country_list_dict[item['country']]=item['states']
        return country_list_dict

  ## main job where it will run job for each state along with retry for 3 in case state fails,\
  ## in case retry of schedular it will starts from where the states needs to be performed
    def my_job(self,country,country_data_dict):
        try:
            retry_count=3
            states_data=country_data_dict[country]
            
            Failure_state=self.Failure_state_retry.get(country,[])
            if isinstance(Failure_state, (int, float)):
                states_data=states_data[self.Failure_state_retry[country]:]

           
            for i in range(len(states_data)):
                
                for z in range(retry_count):
                    try:
                        time.sleep(1)
                        print(states_data[i])
                        break
                    except Exception as e:
                        if z==2:
                            self.Failure_state_retry[country]=i
                            self.Status[country]='Failure'
                            return
                        else:
                            continue
                    
            self.Status[country]='Success'
        except Exception as e:
            self.Status[country]='Failure'
            print(e)
            
   ## this function is used to trigger the schedular for job
    def run(self,id):
        country_data_dict=self.get_the_countries(self.input_data)
        country_list=country_data_dict.keys()

        thread_list = []
        for country in country_list:
            t=threading.Thread(target=self.my_job, args=(country,country_data_dict))
            thread_list.append(t)

        for thread in thread_list:
            thread.start()
            
        for thread in thread_list:
            thread.join()
    
    ##function to check job status 
    def job_status_schedular(self):
        
        for value in self.Status.values():
            if value=='Failure':
                self.job_status['status']=False
                break
            else:
                self.job_status['status']=True
        return self.job_status['status']







input_data=[
  {  
    "country": "India",
    "states": ["AP", "UP", "Bihar"]
  },
  { 
    "country": "US",
    "states": ["California", "Washington", "New York"]
  }
]

schedular1=schedular(input_data,1)   
schedular1.run(schedular1.idempotency_id)
status=schedular1.job_status_schedular()

if status==True:
    print("Job runs successfull")
else:
    print("Job has failed retry once")
    schedular1.run()
    
print(schedular1.job_status_schedular())