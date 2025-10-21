import { format } from "date-fns";

export function getBogotaDateString(formatString) {
  return format(new Date(), formatString);
}
