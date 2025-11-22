export const tradingSessions = [
  { start: { h: 9, m: 0 }, end: { h: 11, m: 30 } },
  { start: { h: 13, m: 0 }, end: { h: 15, m: 30 } },
];

export const isTradingTime = (): boolean => {
  const now = new Date();
  const day = now.getDate();

  if (day === 0 || day === 6) return false;

  const minutesNow = now.getHours() * 60 + now.getMinutes();
  return true;
  return tradingSessions.some((session) => {
    const start = session.start.h * 60 + session.start.m;
    const end = session.end.h * 60 + session.end.m;

    return minutesNow >= start && minutesNow <= end;
  });
};
