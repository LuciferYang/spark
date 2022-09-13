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

package org.apache.spark;

import java.util.StringJoiner;

public class TestApis {

    public long sum(long p0, long p1, long p2, long p3, long p4, long p5, long p6, long p7, long p8,
        long p9, long p10, long p11, long p12, long p13, long p14, long p15, long p16, long p17, long p18,
        long p19, long p20, long p21, long p22, long p23, long p24, long p25, long p26, long p27, long p28,
        long p29, long p30, long p31, long p32, long p33, long p34, long p35, long p36, long p37, long p38,
        long p39, long p40, long p41, long p42, long p43, long p44, long p45, long p46, long p47, long p48,
        long p49, long p50, long p51, long p52, long p53, long p54, long p55, long p56, long p57, long p58,
        long p59, long p60, long p61, long p62, long p63, long p64, long p65, long p66, long p67, long p68,
        long p69, long p70, long p71, long p72, long p73, long p74, long p75, long p76, long p77, long p78,
        long p79, long p80, long p81, long p82, long p83, long p84, long p85, long p86, long p87, long p88,
        long p89, long p90, long p91, long p92, long p93, long p94, long p95, long p96, long p97, long p98,
        long p99, long p100, long p101, long p102, long p103, long p104, long p105, long p106, long p107,
        long p108, long p109, long p110, long p111, long p112, long p113, long p114, long p115, long p116,
        long p117, long p118, long p119, long p120, long p121, long p122, long p123, long p124, long p125,
        long p126) {

        return p0 + p1 + p2 + p3 + p4 + p5 + p6 + p7 + p8 + p9 + p10 + p11 + p12 + p13 + p14 + p15 + p16 +
          p17 + p18 + p19 + p20 + p21 + p22 + p23 + p24 + p25 + p26 + p27 + p28 + p29 + p30 + p31 + p32 +
          p33 + p34 + p35 + p36 + p37 + p38 + p39 + p40 + p41 + p42 + p43 + p44 + p45 + p46 + p47 + p48 +
          p49 + p50 + p51 + p52 + p53 + p54 + p55 + p56 + p57 + p58 + p59 + p60 + p61 + p62 + p63 + p64 +
          p65 + p66 + p67 + p68 + p69 + p70 + p71 + p72 + p73 + p74 + p75 + p76 + p77 + p78 + p79 + p80 +
          p81 + p82 + p83 + p84 + p85 + p86 + p87 + p88 + p89 + p90 + p91 + p92 + p93 + p94 + p95 + p96 +
          p97 + p98 + p99 + p100 + p101 + p102 + p103 + p104 + p105 + p106 + p107 + p108 + p109 + p110 +
          p111 + p112 + p113 + p114 + p115 + p116 + p117 + p118 + p119 + p120 + p121 + p122 + p123 + p124 +
          p125 + p126;
    }
    
    public long sum(TestParameters p) {
        return p.p0 + p.p1 + p.p2 + p.p3 + p.p4 + p.p5 + p.p6 + p.p7 + p.p8 + p.p9 + p.p10 + p.p11 + p.p12 +
          p.p13 + p.p14 + p.p15 + p.p16 + p.p17 + p.p18 + p.p19 + p.p20 + p.p21 + p.p22 + p.p23 + p.p24 +
          p.p25 + p.p26 + p.p27 + p.p28 + p.p29 + p.p30 + p.p31 + p.p32 + p.p33 + p.p34 + p.p35 + p.p36 +
          p.p37 + p.p38 + p.p39 + p.p40 + p.p41 + p.p42 + p.p43 + p.p44 + p.p45 + p.p46 + p.p47 + p.p48 +
          p.p49 + p.p50 + p.p51 + p.p52 + p.p53 + p.p54 + p.p55 + p.p56 + p.p57 + p.p58 + p.p59 + p.p60 +
          p.p61 + p.p62 + p.p63 + p.p64 + p.p65 + p.p66 + p.p67 + p.p68 + p.p69 + p.p70 + p.p71 + p.p72 +
          p.p73 + p.p74 + p.p75 + p.p76 + p.p77 + p.p78 + p.p79 + p.p80 + p.p81 + p.p82 + p.p83 + p.p84 +
          p.p85 + p.p86 + p.p87 + p.p88 + p.p89 + p.p90 + p.p91 + p.p92 + p.p93 + p.p94 + p.p95 + p.p96 +
          p.p97 + p.p98 + p.p99 + p.p100 + p.p101 + p.p102 + p.p103 + p.p104 + p.p105 + p.p106 + p.p107 +
          p.p108 + p.p109 + p.p110 + p.p111 + p.p112 + p.p113 + p.p114 + p.p115 + p.p116 + p.p117 + p.p118 +
          p.p119 + p.p120 + p.p121 + p.p122 + p.p123 + p.p124 + p.p125 + p.p126;
    }

    public static void main(String[] args) {
        StringJoiner joiner = new StringJoiner(".");
        for (int i = 0; i < 127; i++) {
            joiner.add("withP" + i + "(data(" + i + "))");
        }
        System.out.println(joiner);
    }
}
