FROM google/cloud-sdk:latest
RUN pip install pandas
RUN pip install google-cloud-storage
RUN pip install google-api-python-client
RUN pip install sklearn


COPY ./ /root/

RUN chmod 777 -R /root
ENTRYPOINT [ "python", "/root/find_threshold.py" ]