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
  Button,
  Flex,
  Link,
  Divider,
} from '@chakra-ui/react';

import { useExtraLinks } from '../../../api';

const ExtraLinks = ({
  dagId,
  taskId,
  executionDate,
  extraLinks = [],
}) => {
  const { data: links = [] } = useExtraLinks({
    dagId, taskId, executionDate, extraLinks,
  });

  if (!links.length) return null;
  const isExternal = (url) => /^(?:[a-z]+:)?\/\//.test(url);

  return (
    <>
      <Divider my={2} />
      <Flex flexWrap="wrap">
        {links.map(({ name, url }) => (
          <Button
            key={name}
            as={Link}
            colorScheme="blue"
            href={url}
            isDisabled={!url}
            target={isExternal(url) ? '_blank' : undefined}
          >
            {name}
          </Button>
        ))}
      </Flex>
    </>
  );
};

export default ExtraLinks;
