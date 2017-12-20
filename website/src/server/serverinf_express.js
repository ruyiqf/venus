const express = require('express')
const app = express()
const port = 8081

app.get('/interface/search/all', (request, response) => {
    var blogs = [{"title": "euxyacg0"}, {"title":"euxyacg1"}]
    response.responseType='json'
    response.send(blogs)
})

app.listen(port, (err) => {
    if (err) {
        return console.log('something bad happened', err)
    }

    console.log(`server is listening on ${port}`)
})
