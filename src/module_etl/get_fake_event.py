from faker import Faker 
import numpy as np
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from time import time
import get_data

# Gera os arquivos fakes
fake = Faker() 

data_fake = get_data.data_fake()
str_date = str(data_fake[0:4])+'-'+str(data_fake[4:6])+'-'+str(data_fake[6:8])
date_obj = datetime.strptime(str_date, '%Y-%m-%d').date()
date_obj

def create_data(x): 
  
    # dictionary 
    fake_data ={} 
    for i in range(0, x): 
        fake_data[i]={} 
        fake_data[i]['name'] = fake.name() 
        fake_data[i]['first_name'] = fake.first_name() 
        fake_data[i]['last_name'] = fake.last_name() 
        fake_data[i]['address'] = fake.address() 
        fake_data[i]['city'] = fake.city() 
        fake_data[i]['profile'] = [fake.simple_profile()]
        fake_data[i]['type'] = fake.word()
        fake_data[i]['job'] = fake.job()
        fake_data[i]['quantity'] = np.random.randint(1,15)
        fake_data[i]['value'] = np.random.randint(50,250)
        fake_data[i]['ip'] = fake.ipv4()
        fake_data[i]['Host_name'] = fake.hostname()
        fake_data[i]['Domain_name'] = fake.domain_name()
        fake_data[i]['Domain_word'] = fake.domain_word()
        fake_data[i]['TLD'] = fake.tld()
        fake_data[i]['image_url'] = fake.image_url()
        fake_data[i]['first_access'] = fake.past_date()
        # fake_data[i]['last_access'] = date_obj
        fake_data[i]['last_access'] = date.today()

    return fake_data