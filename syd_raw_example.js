const request = require('request');

const options = {
    url: 'https://www.sydneyairport.com.au/_a/flights',
    qs: {
        flightType: 'departure',
        terminalType: 'international',
        filter: '',
        date: '2024-01-27',
        count: '100',
        startFrom: '0',
        seq: '1',
        sortColumn: 'scheduled_time',
        ascending: 'true',
        showAll: 'true'
    },
    headers: {
        'accept': 'application/json',
    }
};

request.get(options, function (error, response, body) {
    if (error) throw new Error(error);

    console.log(JSON.parse(body));
});
