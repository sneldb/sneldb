import { describe, expect, it } from "vitest";

import { extractErrorMessage, parseAndNormalizeResponse } from "../src/parser";
import type { TransportResponse } from "../src/transport/base";

describe("parser", () => {
  const response = (body: string): TransportResponse => ({
    status: 200,
    body,
    headers: {}
  });

  it("parses streaming JSON frames", () => {
    const body = [
      '{"type":"schema","columns":[{"name":"id"},{"name":"value"}]}',
      '{"type":"row","values":{"id":1,"value":"foo"}}',
      '{"type":"row","values":[2,"bar"]}',
      '{"type":"end","row_count":2}'
    ].join("\n");

    const rows = parseAndNormalizeResponse(response(body), "text");
    expect(rows).toEqual([
      { id: 1, value: "foo" },
      { id: 2, value: "bar" }
    ]);
  });

  it("parses JSON arrays", () => {
    const rows = parseAndNormalizeResponse(response('[{"foo": "bar"}]'), "json");
    expect(rows).toEqual([{ foo: "bar" }]);
  });

  it("parses pipe-delimited output", () => {
    const body = [
      "ID|VALUE",
      "1|foo",
      "2|bar"
    ].join("\n");

    const rows = parseAndNormalizeResponse(response(body), "text");
    expect(rows).toEqual([
      { id: "1", value: "foo" },
      { id: "2", value: "bar" }
    ]);
  });

  it("extracts error messages from JSON", () => {
    expect(extractErrorMessage('{"message":"boom"}')).toBe("boom");
    expect(extractErrorMessage("unexpected"))
      .toBe("unexpected");
  });
});
