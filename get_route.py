from __future__ import print_function

import boto3


def lambda_handler(event, context):
    '''Provide an event that contains the following keys:

      - route: route number
    '''

    dyn_routes = boto3.resource('dynamodb').Table('routs')
    dyn_stops = boto3.resource('dynamodb').Table('stops')

    route = event['route']

    response = dyn_routes.get_item(Key={'id': route})

    answer = []

    if response.has_key('Item') and response['Item']:
        item = response['Item']
        if item.has_key('stop'):
            # Check if the route has at least start and end stops
            if len(item['stop']) >= 2:
                # Get geo location of all stops
                geo_stops = []
                for s in item['stop']:
                    # Request stop data from DynamoDB
                    stop_resp = dyn_stops.get_item(Key={'id': s})
                    if stop_resp.has_key('Item') and stop_resp['Item']:
                        stop_data = stop_resp['Item']
                        geo_stops.append({'name': stop_data['name'],
                                          'lat': float(stop_data['lat']),
                                          'lon': float(stop_data['lon'])})
                    else:
                        raise Exception("Bad Request: Location %s not found in DB" % s)
                # Append start stop
                answer.append({'type': 'start',
                               'name': geo_stops[0]['name'],
                               'lat': float(geo_stops[0]['lat']),
                               'lon': float(geo_stops[0]['lon'])})

                # Append end stop
                answer.append({'type': 'end',
                               'name': geo_stops[-1]['name'],
                               'lat': float(geo_stops[-1]['lat']),
                               'lon': float(geo_stops[-1]['lon'])})

                # Check that number of time durations equal to number of remaining stops and iterate over them
                if item.has_key('stop_time') and len(item['stop_time']) == len(geo_stops[1:-1]):
                    for m in geo_stops[1:-1]:
                        answer.append({'type': 'middle',
                                       'name': m['name'],
                                       'lat': float(m['lat']),
                                       'lon': float(m['lon'])})

                    return answer
                else:
                    raise Exception("Bad Request: No stop duration times in DB or wrong number")
            else:
                raise Exception("Bad Request: Route has to include at least 2 stops")
        else:
            raise Exception("Bad Request: Stop list not found in response")
    else:
        raise Exception("Not Found: Route not found")
