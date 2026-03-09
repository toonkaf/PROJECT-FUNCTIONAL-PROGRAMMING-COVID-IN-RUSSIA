// For more information on writing tests, see
// https://scalameta.org/munit/docs/getting-started.html
class MySuite extends munit.FunSuite {
  test("example test that succeeds") {
    val obtained = 42
    val expected = 42
    assertEquals(obtained, expected)
  }

  test("Daily Summary should sum values correctly") {
    val mockData = List(
      CovidRecord("2020-03-06", "Moscow region", 5.0, 0.0, 0.0),
      CovidRecord("2020-03-06", "Moscow region", 1.0, 0.0, 0.0)
    )

    val result = mockData.map(_.confirmed).sum 
    assertEquals(result, 6.0)
  }
}
