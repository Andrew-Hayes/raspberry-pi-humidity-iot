# raspberry-pi-humidity-iot
Code to turn a Raspberry Pi Zero into an IoT humidity sensor

## To run

you just need to execute the command:
```
python3 humidity_iot.py --algorithm RS256 --cloud_region <gcp-region>  \
--device_id <device-id> --project_id <GCP-project-id>  \
--registry_id <gcp-iot-device-registry-id>
```

## Generate certificates

```
openssl req -x509 -nodes -newkey rsa:2048 \
-keyout ./key.pem \
-out crt.pem \
-days 365 \
-subj "/CN=unused"
```

## Download google root certificates from:

https://pki.google.com/roots.pem