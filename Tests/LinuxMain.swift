import XCTest

import MySQLSwORMTests

var tests = [XCTestCaseEntry]()
tests += MySQLSwORMTests.allTests()
XCTMain(tests)