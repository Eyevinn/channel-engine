const constants = {
  ALLOW_HEADERS: [
    'accept',
    'accept-version',
    'content-type',
    'request-id',
    'origin',
    'x-api-version',
    'x-request-id',
    'x-requested-with'
  ],
  EXPOSE_HEADERS: [
    'api-version',
    'content-length',
    'content-md5',
    'content-type',
    'date',
    'request-id',
    'response-time'
  ],
  AC_REQ_METHOD: 'access-control-request-method',
  AC_REQ_HEADERS: 'access-control-request-headers',
  AC_ALLOW_CREDS: 'access-control-allow-credentials',
  AC_ALLOW_ORIGIN: 'access-control-allow-origin',
  AC_ALLOW_HEADERS: 'access-control-allow-headers',
  AC_ALLOW_METHODS: 'access-control-allow-methods',
  AC_EXPOSE_HEADERS: 'access-control-expose-headers',
  AC_MAX_AGE: 'access-control-max-age',
  STR_VARY: 'vary',
  STR_ORIGIN: 'origin',
  HTTP_NO_CONTENT: 204
}

exports.handler = function (req, res, next) {
  if (req.method !== 'OPTIONS') return next();
  res.once('header', function () {
    res.header(constants.AC_ALLOW_ORIGIN, '*');
    res.header(constants.AC_ALLOW_CREDS, true);
    res.header(constants.AC_ALLOW_METHODS, ['GET', 'OPTIONS']);
    res.header(constants.AC_ALLOW_HEADERS, ['x-health-key']);
  });

  res.send(constants.HTTP_NO_CONTENT);
}
