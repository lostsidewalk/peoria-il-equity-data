import usaddress


def cleanup_location(location):
    try:
        return usaddress.parse(location) if location else None
    except Exception as e:
        print(f'Error parsing location: {location}')


def cleanup_certifications(certification_information):
    all_certs = []
    certs = certification_information.split('\n')
    for cert in certs:
        cert_parts = cert.split(' - ', 1)
        if len(cert_parts) == 2:
            cert_location, cert_info = cert_parts
            cert_name, cert_expiration = cert_info.split(' expiring on ')
            cert_info = {
                "location": cert_location,
                "name": cert_name,
                "expiration": cert_expiration,
            }
            all_certs.append(cert_info)
    return all_certs


def cleanup_records(records):
    for r in records:
        r['certification_information'] = cleanup_certifications(r.get('certification_information', ''))
        r['location'] = cleanup_location(r.get('location', ''))
