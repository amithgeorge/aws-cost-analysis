function _monthStr(month) {
  return month < 10 ? `0${month}` : `${month}`;
}

function monthStr(date) {
  let month = date.getMonth() + 1;
  return _monthStr(month);
}

function dayOfMonthStr(date) {
  let dayOfMonth = date.getDate();
  return dayOfMonth < 10 ? `0${dayOfMonth}` : `${dayOfMonth}`;
}

function dateStr(date) {
  return `${date.getFullYear()}${monthStr(date)}${dayOfMonthStr(date)}`;
}

function getPeriodStr(year, month) {
  month = month - 1; // in JS Jan is 0, Feb is 1 etc
  let startOfMonth = dateStr(new Date(year, month, 1));
  let startOfNextMonth = dateStr(new Date(year, month + 1, 1));
  return `${startOfMonth}-${startOfNextMonth}`;
}

function getLocalFilePrefix(year, month) {
  return `${year}${_monthStr(month)}`;
}

module.exports = {
  getPeriodStr,
  getLocalFilePrefix
};
