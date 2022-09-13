/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import org.apache.spark.benchmark.BenchmarkBase

object TestApiBenchmark extends BenchmarkBase {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    import org.apache.spark.benchmark.Benchmark
    val valuesPerIteration = 10000000
    val benchmark = new Benchmark("Test sum", valuesPerIteration,
      output = output)


    val apis = new TestApis

    val data = 0L until 127L

    benchmark.addCase("Use multiple parameters method") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        apis.sum(data(0), data(1), data(2), data(3), data(4), data(5), data(6),
          data(7), data(8), data(9), data(10), data(11), data(12), data(13),
          data(14), data(15), data(16), data(17), data(18), data(19), data(20),
          data(21), data(22), data(23), data(24), data(25), data(26), data(27),
          data(28), data(29), data(30), data(31), data(32), data(33), data(34),
          data(35), data(36), data(37), data(38), data(39), data(40), data(41),
          data(42), data(43), data(44), data(45), data(46), data(47), data(48),
          data(49), data(50), data(51), data(52), data(53), data(54), data(55),
          data(56), data(57), data(58), data(59), data(60), data(61), data(62),
          data(63), data(64), data(65), data(66), data(67), data(68), data(69),
          data(70), data(71), data(72), data(73), data(74), data(75), data(76),
          data(77), data(78), data(79), data(80), data(81), data(82), data(83),
          data(84), data(85), data(86), data(87), data(88), data(89), data(90),
          data(91), data(92), data(93), data(94), data(95), data(96), data(97),
          data(98), data(99), data(100), data(101), data(102), data(103), data(104),
          data(105), data(106), data(107), data(108), data(109), data(110), data(111),
          data(112), data(113), data(114), data(115), data(116), data(117), data(118),
          data(119), data(120), data(121), data(122), data(123), data(124), data(125),
          data(126))
      }
    }

    benchmark.addCase("Use TestParameters create new") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val parameters = new TestParameters(
          data(0), data(1), data(2), data(3), data(4), data(5), data(6),
          data(7), data(8), data(9), data(10), data(11), data(12), data(13), data(14),
          data(15), data(16), data(17), data(18), data(19), data(20), data(21), data(22),
          data(23), data(24), data(25), data(26), data(27), data(28), data(29), data(30),
          data(31), data(32), data(33), data(34), data(35), data(36), data(37), data(38),
          data(39), data(40), data(41), data(42), data(43), data(44), data(45), data(46),
          data(47), data(48), data(49), data(50), data(51), data(52), data(53), data(54),
          data(55), data(56), data(57), data(58), data(59), data(60), data(61), data(62),
          data(63), data(64), data(65), data(66), data(67), data(68), data(69), data(70),
          data(71), data(72), data(73), data(74), data(75), data(76), data(77), data(78),
          data(79), data(80), data(81), data(82), data(83), data(84), data(85), data(86),
          data(87), data(88), data(89), data(90), data(91), data(92), data(93), data(94),
          data(95), data(96), data(97), data(98), data(99), data(100), data(101), data(102),
          data(103), data(104), data(105), data(106), data(107), data(108), data(109),
          data(110), data(111), data(112), data(113), data(114), data(115), data(116),
          data(117), data(118), data(119), data(120), data(121), data(122), data(123),
          data(124), data(125), data(126))
          apis.sum(parameters)
      }
    }


    benchmark.addCase("Use TestParameters reuse") { _: Int =>
      val parameters = new TestParameters()
      for (_ <- 0L until valuesPerIteration) {
        val input = parameters.withP0(data(0)).withP1(data(1)).withP2(data(2))
          .withP3(data(3)).withP4(data(4)).withP5(data(5)).withP6(data(6))
          .withP7(data(7)).withP8(data(8)).withP9(data(9)).withP10(data(10))
          .withP11(data(11)).withP12(data(12)).withP13(data(13)).withP14(data(14))
          .withP15(data(15)).withP16(data(16)).withP17(data(17)).withP18(data(18))
          .withP19(data(19)).withP20(data(20)).withP21(data(21)).withP22(data(22))
          .withP23(data(23)).withP24(data(24)).withP25(data(25)).withP26(data(26))
          .withP27(data(27)).withP28(data(28)).withP29(data(29)).withP30(data(30))
          .withP31(data(31)).withP32(data(32)).withP33(data(33)).withP34(data(34))
          .withP35(data(35)).withP36(data(36)).withP37(data(37)).withP38(data(38))
          .withP39(data(39)).withP40(data(40)).withP41(data(41)).withP42(data(42))
          .withP43(data(43)).withP44(data(44)).withP45(data(45)).withP46(data(46))
          .withP47(data(47)).withP48(data(48)).withP49(data(49)).withP50(data(50))
          .withP51(data(51)).withP52(data(52)).withP53(data(53)).withP54(data(54))
          .withP55(data(55)).withP56(data(56)).withP57(data(57)).withP58(data(58))
          .withP59(data(59)).withP60(data(60)).withP61(data(61)).withP62(data(62))
          .withP63(data(63)).withP64(data(64)).withP65(data(65)).withP66(data(66))
          .withP67(data(67)).withP68(data(68)).withP69(data(69)).withP70(data(70))
          .withP71(data(71)).withP72(data(72)).withP73(data(73)).withP74(data(74))
          .withP75(data(75)).withP76(data(76)).withP77(data(77)).withP78(data(78))
          .withP79(data(79)).withP80(data(80)).withP81(data(81)).withP82(data(82))
          .withP83(data(83)).withP84(data(84)).withP85(data(85)).withP86(data(86))
          .withP87(data(87)).withP88(data(88)).withP89(data(89)).withP90(data(90))
          .withP91(data(91)).withP92(data(92)).withP93(data(93)).withP94(data(94))
          .withP95(data(95)).withP96(data(96)).withP97(data(97)).withP98(data(98))
          .withP99(data(99)).withP100(data(100)).withP101(data(101)).withP102(data(102))
          .withP103(data(103)).withP104(data(104)).withP105(data(105)).withP106(data(106))
          .withP107(data(107)).withP108(data(108)).withP109(data(109)).withP110(data(110))
          .withP111(data(111)).withP112(data(112)).withP113(data(113)).withP114(data(114))
          .withP115(data(115)).withP116(data(116)).withP117(data(117)).withP118(data(118))
          .withP119(data(119)).withP120(data(120)).withP121(data(121)).withP122(data(122))
          .withP123(data(123)).withP124(data(124)).withP125(data(125)).withP126(data(126))
        apis.sum(input)
      }
    }

    benchmark.addCase("Use data sum") { _: Int =>
      for (_ <- 0L until valuesPerIteration) {
        val sum = data.sum
      }
    }

    benchmark.run()
  }
}
