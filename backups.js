// Проход по всем ключам и значениям объекта
Object.keys(flight).forEach((key) => {
    // Получаем значение текущего ключа
    let value = flight[key];
    // Проверяем, является ли значение строкой
    if (typeof value === 'string') {
        // Удаляем все символы, кроме цифр, пробелов и букв
        value = value.replace(/[^\d\s\w]/gi, '');
        // Записываем очищенное значение обратно в объект
        flight[key] = value;
    } return flight;
});
// Пройдемся по всем ключам и значениям объекта и заменим символы, не соответствующие шаблону
Object.keys(flight).forEach((key) => {
    if (typeof flight[key] === 'string') {
        flight[key] = flight[key].replace(/[^A-Za-z0-9\s]/g, ''); // Замена всех символов, не являющихся буквами, цифрами и пробелами
    } return flight;
});

// Фильтруем по статусу и добавляем родителя в массив
if (status === 1 ? flight : status === 0 ? flight : null) {
    console.log('Adding parent:', flights_bucket);
    newFlightsArray.push(flights_bucket);
    // Добавляем детей в массив, если они есть
    if (flight.codeshare && flight.codeshare.length > 0) {
        console.log('Adding children:', flight.codeshare);
        newFlightsArray.push(...flight.codeshare.map((code) => ({
            ...flights_bucket,
            'cs_airline_iata': flights_bucket.airline_iata || null,
            'cs_flight_number': flights_bucket.flight_number || null,
            'cs_flight_iata': flights_bucket.flight_iata || null,
            'airline_iata': status === 0 ? code.slice(0, 2) : status === 1 ? code.slice(0, 2) : null,
            'flight_iata': status === 0 ? code : status === 1 ? code : null,
            'flight_number': status === 0 ? code.slice(2, 6) : status === 1 ? code.slice(2, 6) : null,
        })));
    }
    return flights_bucket;
}
function findAndRemoveDuplicatesByKey(arr, key) {
    const duplicates = [];
    const uniqueItems = [];
    for (let i = 0; i < arr.length; i++) {
        let isDuplicate = false;
        for (let j = i + 1; j < arr.length; j++) {
            if (arr[i][key] === arr[j][key]) {
                duplicates.push({ duplicate1: arr[i], duplicate2: arr[j] });
                isDuplicate = true;
                break;
            }
        }
        if (!isDuplicate) {
            uniqueItems.push(arr[i]);
        }
    }
    return { duplicates, uniqueItems };
}
const { duplicates, uniqueItems } = findAndRemoveDuplicatesByKey(newFlightsArray, 'flight_iata');
if (duplicates.length > 0) {
    console.log('Duplicates found:', duplicates);
    console.log('Unique items:', uniqueItems);
    //Удаляю дубликаты из исходного массива, заменив его уникальными рейсами
    newFlightsArray = uniqueItems;
} else {
    console.log('No duplicates found.');
}

if (newFlightsFields.length >= max_page_size) {
    finished = false;
    console.log('Loop is still running...'.bgYellow);
} else {
    finished = true;
    console.log('Loop is over.'.bgBlue);
}
retry_done();

// Добавление рейсов в массив
if (arrival ? flight : departure ? flight : null) {
    syd_flights.push(spread_fields);
    // Проверка наличия номеров рейсов и добавление дочерних рейсов
    if (flight.flightNumbers && flight.flightNumbers.length > 0) {
        flight.flightNumbers.forEach((number, index) => {
            if (index === 0) {
                return; // Пропускаем первый номер, так как он уже добавлен как родительский рейс
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

const regExp_flight = {};
const exp = /^[a-z\d]$/im;
if (exp.test(flight.airlineCode) &&
    exp.test(flight.scheduledTime) &&
    exp.test(flight.scheduledDate) &&
    exp.test(flight.estimatedTime) &&
    exp.test(flight.estimatedDate) &&
    exp.test(flight.status)) {
    regExp_flight.airline_iata = String(flight.airlineCode).toUpperCase();
    regExp_flight.scheduledTime = String(flight.scheduledDate && flight.scheduledTime);
    regExp_flight.estimatedTime = String(flight.estimatedDate && flight.estimatedTime);
    regExp_flight.status = String(flight.status).toLowerCase();
} else {
    regExp_flight.airline_iata = null;
    regExp_flight.scheduledTime = null;
    regExp_flight.estimatedTime = null;
    regExp_flight.status = null;
} return regExp_flight;