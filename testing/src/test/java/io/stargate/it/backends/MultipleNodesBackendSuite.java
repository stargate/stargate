/// *
// * Copyright The Stargate Authors
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
// package io.stargate.it.backends;
//
// import io.stargate.it.MultipleStargateInstancesTest;
// import org.junit.runners.Parameterized;
// import org.junit.runners.Suite;
//
/// **
// * Allows a suite of tests to run against a number of persistence backends. Each backend should
// * extend this class.
// */
// @Suite.SuiteClasses({MultipleStargateInstancesTest.class})
// public abstract class MultipleNodesBackendSuite {
//  @Parameterized.Parameter(0)
//  public String dockerImage;
//
//  @Parameterized.Parameter(1)
//  public Boolean isDse;
//
//  @Parameterized.Parameter(2)
//  public String version;
//
//  @Parameterized.Parameter(3)
//  public Integer numberOfStargateNodes;
// }
