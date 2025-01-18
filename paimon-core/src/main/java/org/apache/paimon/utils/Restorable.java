/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.utils;

import org.apache.paimon.annotation.Public;

/**
 * Operations implementing this interface can checkpoint and restore their states between different
 * instances.
 *
 * @param <S> type of state
 * @since 0.4.0
 */
/**
* @授课老师: 码界探索
* @微信: 252810631
* @版权所有: 请尊重劳动成果
* 实现此接口的操作可以在不同实例之间检查和恢复其状态。
*/
@Public
public interface Restorable<S> {

    /** Extract state of the current operation instance. */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 提取当前操作实例的状态
    */
    S checkpoint();

    /** Restore state of a previous operation instance into the current operation instance. */
    /**
    * @授课老师: 码界探索
    * @微信: 252810631
    * @版权所有: 请尊重劳动成果
    * 将上一个操作实例的状态还原到当前操作实例中。
    */
    void restore(S state);
}
