import { ParseError } from "./errors";
import type { OutputFormat, NormalizedRecord } from "./types";
import type { TransportResponse } from "./transport/base";

export function parseAndNormalizeResponse(
  response: TransportResponse,
  _outputFormat: OutputFormat
): NormalizedRecord[] {
  return parseTextToRecords(response.body ?? "");
}

export function extractErrorMessage(body: string): string {
  if (!body) {
    return "Error occurred";
  }

  try {
    const parsed = JSON.parse(body);
    if (parsed && typeof parsed === "object" && "message" in parsed) {
      return String((parsed as { message: unknown }).message);
    }
  } catch {
    // Not JSON, fall through
  }

  return body.trim() || "Error occurred";
}

function parseTextToRecords(textData: string): NormalizedRecord[] {
  const text = (textData ?? "").toString();
  const trimmed = text.trim();
  if (!trimmed) {
    return [];
  }

  const rawLines = text.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const dataLines = dropStatusPrefix(rawLines);

  if (dataLines.length > 0 && dataLines[0].startsWith("{") && dataLines[0].includes("\"type\"")) {
    return parseStreamingJsonResponse(dataLines);
  }

  if (looksLikeJsonObject(trimmed) || looksLikeJsonArray(trimmed)) {
    try {
      const parsed = JSON.parse(trimmed);
      if (Array.isArray(parsed)) {
        return parsed as NormalizedRecord[];
      }
      if (parsed && typeof parsed === "object") {
        return [parsed as NormalizedRecord];
      }
    } catch (error) {
      throw new ParseError(`Invalid JSON response: ${(error as Error).message}`);
    }
  }

  const lines = dataLines;
  if (lines.length === 0) {
    return [];
  }

  if (lines[0].includes("|")) {
    return parsePipeDelimited(lines);
  }

  return lines.map((line) => ({ raw: line }));
}

function dropStatusPrefix(lines: string[]): string[] {
  if (lines.length === 0) {
    return lines;
  }

  const [first, ...rest] = lines;
  if (/^(?:OK|200\s+OK|\d{3}\s+.*)$/i.test(first)) {
    return rest;
  }
  return lines;
}

function looksLikeJsonObject(text: string): boolean {
  return text.startsWith("{") && text.endsWith("}");
}

function looksLikeJsonArray(text: string): boolean {
  return text.startsWith("[") && text.endsWith("]");
}

function parsePipeDelimited(lines: string[]): NormalizedRecord[] {
  const headers = lines[0].split("|").map((header) => header.trim());
  let dataStartIndex = 0;

  if (headers.length > 0 && headers.every((header) => /^[A-Z0-9_]+$/.test(header)) && lines.length > 1) {
    dataStartIndex = 1;
  }

  const results: NormalizedRecord[] = [];
  for (let i = dataStartIndex; i < lines.length; i += 1) {
    const values = lines[i].split("|").map((value) => value.trim());
    if (dataStartIndex > 0 && values.length === headers.length) {
      const record: NormalizedRecord = {};
      headers.forEach((header, idx) => {
        record[header.toLowerCase()] = values[idx];
      });
      results.push(record);
    } else {
      results.push({ raw: lines[i], parts: values });
    }
  }

  return results;
}

function parseStreamingJsonResponse(lines: string[]): NormalizedRecord[] {
  const records: NormalizedRecord[] = [];
  let schema: Array<{ name: string }> | undefined;

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }

    const frame = JSON.parse(trimmed) as { type: string; [key: string]: unknown };
    switch (frame.type) {
      case "schema":
        if (Array.isArray(frame.columns)) {
          schema = frame.columns as Array<{ name: string }>;
        }
        break;
      case "batch": {
        const rows = frame.rows as unknown;
        if (Array.isArray(rows)) {
          for (const row of rows) {
            if (Array.isArray(row) && schema) {
              const record: NormalizedRecord = {};
              row.forEach((value, idx) => {
                const columnName = schema?.[idx]?.name ?? `col_${idx}`;
                record[columnName] = value;
              });
              records.push(record);
            } else if (Array.isArray(row)) {
              records.push({ values: row });
            }
          }
        }
        break;
      }
      case "row": {
        const values = frame.values as unknown;
        if (values && typeof values === "object" && !Array.isArray(values)) {
          records.push(values as NormalizedRecord);
        } else if (Array.isArray(values) && schema) {
          const record: NormalizedRecord = {};
          values.forEach((value, idx) => {
            const columnName = schema?.[idx]?.name ?? `col_${idx}`;
            record[columnName] = value;
          });
          records.push(record);
        } else if (Array.isArray(values)) {
          records.push({ values });
        }
        break;
      }
      case "end":
        return records;
      default:
        break;
    }
  }

  return records;
}
