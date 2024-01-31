const _ = require('lodash');
const async = require('async');
const express = require('express');
const moment = require('moment-timezone');
const request = require('request');

const port = 11254;
const base_url = 'https://www.sydneyairport.com.au/_a/flights';
const headers = { 'accept': 'application/json' };
const proxy = 'http://speej9xhkw:KbdCbB22xmdmxpG28k@dc.smartproxy.com:10000';
const frmt = 'YYYY-MM-DD HH:mm'
const y_m_d = 'YYYY-MM-DD';
const h_m = 'HH:mm'
const tmzn = 'Australia/Sydney';
const day = moment().tz(tmzn);
const dates = [day.format(y_m_d), day.clone().add(1, 'd').format(y_m_d)];

const redis_url = 'redis://127.0.0.1:6379';
const redis_key = 'airports:sydney';

const redis = require('redis')
    .createClient({ url: redis_url, })
    .on('connect', () => {
        console.log(`[${day}][redis] connected.`);
        dataFlights();
    })
    .on('reconnecting', (p) => console.log(`[${day}][redis] reconnecting: %j`, p))
    .on('error', (e) => console.error(`[${day}][redis] error: %j`, e));

redis.del(redis_key, (err, reply) => {
    if (err) {
        console.error(`[${day}][redis] delete error: %j`, err);
    } else {
        console.log(`[${day}][redis] data deleted.`);
    }
});

const app = express();
app.get('/schedules', (req, res) => {
    redis.get(redis_key, (err, reply) => {
        const data = JSON.parse(reply);
        if (!reply) {
            console.error(`[${day}] Data not found.`, err)
            return res.status(404).json({ err: 'Data not found.' });
        }
        try {
            res.json({
                message: 'success',
                status: 200,
                data: {
                    result: data
                }
            });
        } catch (err) {
            console.error(`[${day}] Server error: %j`, err);
            res.status(500).json({ err: 'Server error.' });
        }
    })
}).listen(port, () => { console.log(`Server started on port ${port}`); });

const redisSet = (redis_key, syd_flights, callback) => {
    redis.set(redis_key, JSON.stringify(syd_flights), (err) => {
        if (err) {
            console.error(`[${day}][redis] set error: %j`, err);
            return callback && callback(err);
        } else {
            console.log(`[${day}][redis] data set.`);
        }
        callback && callback();
    })
};

const killSignal = () => {
    console.log('Kill signal.')
    redis && redis.end && redis.end(true);
    setTimeout(() => {
        console.error('Forcing kill signal.');
        return process.exit(1);
    }, 5000);
};
process.once('SIGTERM', killSignal);
process.once('SIGINT', killSignal);

function dataFlights() {
    const body_size = 10;
    const page_size = 100;
    const retries = 5;

    let syd_flights = [];
    let finished = false;
    let page = 1;
    let tries = 0;

    async.eachLimit(dates, 20, (date, next_date) => {
        async.each(['arrival', 'departure'], (type, next_type) => {
            async.each(['domestic', 'international'], (term, next_term) => {
                async.until((test) => { test(null, finished) }, (until_done) => {
                    async.retry(retries, (retry_done) => {
                        if (tries) console.log(`[Retrying#${tries}] [${base_url}]`);
                        tries++;

                        const options = {
                            url: base_url,
                            proxy: proxy,
                            headers: headers,
                            qs: {
                                flightType: type,
                                terminalType: term,
                                date: date,
                                count: page_size,
                                startFrom: '0',
                                sortColumn: 'scheduled_time',
                                ascending: 'true',
                                showAll: 'true'
                            }
                        }
                        request.get(options, (err, res, body) => {
                            if (err || !body || body.length < body_size) {
                                console.error(`[${day}] Request failed: %j`, err);
                                return retry_done(true);
                            } page++;
                            try {
                                const obj = JSON.parse(body);
                                if (!obj || !obj.flightData) { return retry_done(true) }

                                const flights_array = _.get(obj, 'flightData', []);
                                const flights_fields = _.flatMap(flights_array, (flight) => {
                                    const arrival = type === 'arrival';
                                    const departure = type === 'departure';
                                    const isValidDateTime = (date, time) => { return date !== '-' && time !== '-' && date !== '' && time !== '' };
                                    const scheduledDateTime = isValidDateTime(flight.scheduledDate, flight.scheduledTime)
                                        ? moment(`${flight.scheduledDate} ${flight.scheduledTime}`, frmt).format(frmt) : null;
                                    const estimatedDateTime = isValidDateTime(flight.estimatedDate, flight.estimatedTime)
                                        ? moment(`${flight.estimatedDate} ${flight.estimatedTime}`, frmt).format(frmt) : null;

                                    const info_fields = {
                                        'airline_iata': arrival ? flight.airlineCode : departure ? flight.airlineCode : null,
                                        'flight_iata': arrival ? flight.flightNumbers[0] : departure ? flight.flightNumbers[0] : null,
                                        'flight_number': arrival ? flight.flightNumbers[0].slice(2) : departure ? flight.flightNumbers[0].slice(2) : null,
                                        'status': flight.status.toLowerCase(),
                                        'delayed': arrival ? (Math.abs(moment(flight.scheduledTime, h_m).diff(moment(flight.estimatedTime, h_m), 'minutes')) || null) : null ||
                                            departure ? (Math.abs(moment(flight.scheduledTime, h_m).diff(moment(flight.estimatedTime, h_m), 'minutes')) || null) : null,
                                    };
                                    const flight_numbers = {
                                        'cs_airline_iata': null,
                                        'cs_flight_iata': null,
                                        'cs_flight_number': null,
                                    };
                                    const arrival_fields = {
                                        'arr_time': arrival ? scheduledDateTime : null,
                                        'arr_time_ts': arrival ? moment(scheduledDateTime).tz(tmzn).unix() || null : null,
                                        'arr_time_utc': arrival ? moment.tz(scheduledDateTime, tmzn).utc().format(frmt) : null,
                                        'arr_estimated': arrival ? estimatedDateTime : null,
                                        'arr_estimated_ts': arrival ? moment(estimatedDateTime).tz(tmzn).unix() || null : null,

                                        'arr_estimated_utc': arrival ? moment.tz(estimatedDateTime, tmzn).utc().format(frmt) : null,
                                    };
                                    const departure_fields = {
                                        'dep_time': departure ? scheduledDateTime : null,
                                        'dep_time_ts': departure ? moment(scheduledDateTime).tz(tmzn).unix() || null : null,
                                        'dep_time_utc': departure ? moment.tz(scheduledDateTime, tmzn).utc().format(frmt) : null,
                                        'dep_estimated': departure ? estimatedDateTime : null,
                                        'dep_estimated_ts': departure ? moment(estimatedDateTime).tz(tmzn).unix() : null,

                                        'dep_estimated_utc': departure ? moment.tz(estimatedDateTime, tmzn).utc().format(frmt) : null,
                                    };
                                    const spread_fields = {
                                        ...info_fields,
                                        ...flight_numbers,
                                        ...arrival_fields,
                                        ...departure_fields,
                                    };
                                    if (arrival ? flight : departure ? flight : null) {
                                        // Добавляем родительский рейс
                                        syd_flights.push(spread_fields);
                                        // Проверяем наличие номеров рейсов и итерируемся по ним
                                        if (flight.flightNumbers && flight.flightNumbers.length > 0) {
                                            flight.flightNumbers.forEach((number, index) => {
                                                if (index === 0) {
                                                    // Пропускаем первый номер, так как он уже добавлен как родительский рейс, тоесть ничего не возвращаем
                                                    return;
                                                }
                                                // Добавляем дочерние рейсы
                                                syd_flights.push({
                                                    ...spread_fields,
                                                    'cs_airline_iata': spread_fields.airline_iata,
                                                    'cs_flight_number': spread_fields.flight_number,
                                                    'cs_flight_iata': spread_fields.flight_iata,
                                                    'airline_iata': arrival ? number.slice(0, 2) : departure ? number.slice(0, 2) : null,
                                                    'flight_iata': arrival ? number : departure ? number : null,
                                                    'flight_number': arrival ? number.slice(2, 6) : departure ? number.slice(2, 6) : null,
                                                });
                                            });
                                        }
                                    }
                                });

                                function unique_flights(arr, key) {
                                    const duplicates = [];
                                    const unique = [];
                                    for (let i = 0; i < arr.length; i++) {
                                        let is_duplicate = false;
                                        for (let j = i + 1; j < arr.length; j++) {
                                            if (arr[i][key] === arr[j][key]) {
                                                duplicates.push({ duplicate1: arr[i], duplicate2: arr[j] });
                                                is_duplicate = true;
                                                break;
                                            }
                                        }
                                        if (!is_duplicate) {
                                            unique.push(arr[i]);
                                        }
                                    } return { duplicates, unique };
                                }

                                const { duplicates, unique } = unique_flights(syd_flights, 'flight_iata');
                                if (duplicates.length > 0) {
                                    syd_flights = unique;
                                } else if (flights_fields.length >= page_size) {
                                    finished = false;
                                } else {
                                    finished = true;
                                } retry_done(true);
                                console.log(syd_flights);
                                return syd_flights;

                            } catch (err) {
                                console.error(`[${day}][error][${err}]`);
                                return retry_done(true);
                            }

                        }, retry_done)
                    }, until_done)
                }, () => {
                    redisSet(redis_key, syd_flights, (err) => {
                        if (err) {
                            console.error('Error saving data:', err);
                        } else {
                            console.log(`[${day}][redis] data saved.`);
                        }
                    });
                }, next_term())
            }, next_type())
        }, next_date())
    });
};