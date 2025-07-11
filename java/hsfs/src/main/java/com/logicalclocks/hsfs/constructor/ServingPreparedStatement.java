/*
 *  Copyright (c) 2021-2023. Hopsworks AB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.logicalclocks.hsfs.constructor;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.logicalclocks.hsfs.metadata.RestDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ServingPreparedStatement extends RestDto<ServingPreparedStatement> {
  @Getter
  @Setter
  private Integer featureGroupId;
  @Getter
  @Setter
  private Integer preparedStatementIndex;
  @Getter
  @Setter
  private List<PreparedStatementParameter> preparedStatementParameters;
  @Getter
  @Setter
  private String queryOnline;
  @Getter
  @Setter
  private String prefix;

  public ServingPreparedStatement(Integer preparedStatementIndex,
                                  List<PreparedStatementParameter> preparedStatementParameters, String queryOnline) {
    this.preparedStatementIndex = preparedStatementIndex;
    this.preparedStatementParameters = preparedStatementParameters;
    this.queryOnline = queryOnline;
  }
}
