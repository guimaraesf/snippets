# Python image to use.
FROM python:3.10.12-slim

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Set the working directory to /app
RUN mkdir -p /usr/src/app

WORKDIR /usr/src/app

# copy the requirements file used for dependencies
COPY requirements.txt /usr/src/app

# Copy the rest of the working directory contents into the container at /app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir --trusted-host pypi.org --trusted-host files.pythonhosted.org -r /usr/src/app/requirements.txt

EXPOSE 8080 

# Start the server when the container launches
CMD [ "python", "main.py" ]
