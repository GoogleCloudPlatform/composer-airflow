/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React from 'react';
import {
  Box,
  Text,
  Flex,
} from '@chakra-ui/react';
import { FiChevronUp, FiChevronDown } from 'react-icons/fi';

const TaskName = ({
  isGroup = false, isMapped = false, onToggle, isOpen, level, taskName,
}) => (
  <Box _groupHover={{ backgroundColor: 'rgba(113, 128, 150, 0.1)' }} transition="background-color 0.2s">
    <Flex
      as={isGroup ? 'button' : 'div'}
      onClick={onToggle}
      color={level > 4 && 'white'}
      aria-label={taskName}
      title={taskName}
      mr={4}
      width="100%"
      backgroundColor={`rgba(203, 213, 224, ${0.25 * level})`}
      alignItems="center"
    >
      <Text
        display="inline"
        fontSize="12px"
        ml={level * 4 + 4}
        isTruncated
      >
        {taskName}
        {isMapped && (
          ' [ ]'
        )}
      </Text>
      {isGroup && (
        isOpen ? <FiChevronDown data-testid="open-group" /> : <FiChevronUp data-testid="closed-group" />
      )}
    </Flex>
  </Box>
);

// Only rerender the component if props change
const MemoizedTaskName = React.memo(TaskName);

export default MemoizedTaskName;
