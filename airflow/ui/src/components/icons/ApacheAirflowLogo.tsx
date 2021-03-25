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

import React, { ReactElement } from 'react';

const ApacheAirflowLogo = ({
  width = '63px',
  height = '24px',
  ...otherProps
}: React.SVGProps<SVGSVGElement>): ReactElement => (
  <svg width={width} height={height} fill="currentColor" xmlns="http://www.w3.org/2000/svg" {...otherProps}>
    <path d="M2.753 5.466a.145.145 0 01-.106-.045.145.145 0 01-.046-.107c0-.04.003-.068.008-.083L4.488.33c.04-.11.12-.166.242-.166h.515c.122 0 .202.055.243.166l1.871 4.902.015.083c0 .04-.015.076-.045.107a.145.145 0 01-.106.045h-.387a.196.196 0 01-.128-.038.238.238 0 01-.06-.09L6.23 4.26H3.745L3.33 5.337a.214.214 0 01-.069.091.183.183 0 01-.12.038h-.387zM6.01 3.61L4.988.92 3.965 3.61h2.046zm2.257 3.295a.175.175 0 01-.13-.053.164.164 0 01-.045-.12V1.7c0-.05.016-.091.046-.122a.175.175 0 01.129-.053h.348c.05 0 .091.018.121.053.036.03.053.071.053.122v.333c.283-.389.698-.583 1.243-.583.53 0 .927.166 1.19.5.267.333.41.762.431 1.288.005.055.008.141.008.257 0 .116-.003.202-.008.258-.02.52-.164.95-.432 1.288-.267.333-.664.5-1.19.5-.524 0-.934-.19-1.226-.568V6.73c0 .05-.016.091-.046.121a.152.152 0 01-.121.053h-.371zm1.606-1.977c.358 0 .618-.111.78-.333.167-.223.258-.515.273-.88.005-.05.008-.123.008-.219 0-.954-.354-1.432-1.061-1.432-.349 0-.611.116-.788.349-.172.227-.265.5-.28.818l-.008.288.008.295c.01.298.106.558.288.78.181.223.441.334.78.334zm3.823.614c-.247 0-.477-.05-.689-.152a1.32 1.32 0 01-.507-.409 1.01 1.01 0 01-.182-.583c0-.334.136-.606.409-.818.278-.218.651-.36 1.121-.425l1.129-.159v-.22c0-.515-.296-.772-.886-.772a.953.953 0 00-.864.47.207.207 0 01-.06.098c-.021.02-.051.03-.092.03h-.325a.18.18 0 01-.122-.045.181.181 0 01-.045-.121c.005-.122.06-.258.167-.41.11-.156.28-.29.507-.401.228-.116.508-.174.841-.174.566 0 .972.133 1.22.401.247.263.371.591.371.985v2.455c0 .05-.018.093-.053.129a.164.164 0 01-.121.045h-.349a.191.191 0 01-.128-.045.191.191 0 01-.046-.13v-.325a1.31 1.31 0 01-.485.41c-.212.11-.482.166-.81.166zm.16-.568c.328 0 .596-.107.803-.319.212-.217.318-.527.318-.932v-.212l-.879.13c-.358.05-.629.136-.81.257-.182.116-.273.265-.273.447 0 .202.083.358.25.47.166.106.363.159.59.159zm4.513.568c-.54 0-.965-.152-1.273-.455-.303-.308-.464-.737-.485-1.288l-.007-.303.007-.303c.02-.55.182-.977.485-1.28.308-.308.733-.462 1.273-.462.364 0 .672.065.924.197.253.13.44.293.561.484.126.192.195.384.205.576.005.05-.01.091-.046.121a.191.191 0 01-.128.046h-.364c-.05 0-.089-.01-.114-.03a.313.313 0 01-.076-.122c-.085-.242-.207-.414-.363-.515a1.031 1.031 0 00-.591-.159c-.313 0-.563.099-.75.296-.182.192-.28.487-.296.886l-.007.273.007.257c.015.404.114.702.296.894.182.192.431.288.75.288.237 0 .434-.05.59-.151.157-.106.279-.28.364-.523a.313.313 0 01.076-.121c.025-.026.063-.038.114-.038h.364c.05 0 .093.018.128.053.036.03.05.07.046.121-.01.187-.079.379-.205.576a1.41 1.41 0 01-.56.485c-.248.131-.556.197-.925.197zm2.771-.076a.191.191 0 01-.128-.045.191.191 0 01-.046-.13V.262c0-.05.015-.09.046-.121a.175.175 0 01.128-.053h.38c.05 0 .09.018.12.053.036.03.053.07.053.121v1.765c.147-.186.319-.328.515-.424.198-.1.442-.151.735-.151.485 0 .862.156 1.13.47.272.307.408.72.408 1.234v2.137c0 .05-.017.093-.053.129a.163.163 0 01-.12.045h-.38a.191.191 0 01-.128-.045.191.191 0 01-.046-.13V3.194c0-.358-.088-.636-.265-.833-.172-.197-.422-.296-.75-.296-.318 0-.573.101-.765.303-.187.197-.28.473-.28.826v2.099c0 .05-.018.093-.054.129a.164.164 0 01-.12.045h-.38zm6.011.076c-.52 0-.937-.16-1.25-.478-.308-.323-.477-.762-.508-1.318l-.007-.257.007-.25c.036-.546.208-.98.515-1.303.314-.324.725-.485 1.235-.485.561 0 .996.179 1.304.538.308.353.462.833.462 1.439v.129c0 .05-.018.093-.053.129a.164.164 0 01-.122.045h-2.621V3.8c.015.329.114.609.296.841.186.227.431.341.734.341.233 0 .422-.045.569-.136a1.15 1.15 0 00.333-.296.41.41 0 01.098-.106.266.266 0 01.13-.023h.37c.046 0 .084.013.114.038a.13.13 0 01.046.106c0 .112-.071.245-.212.402-.137.157-.331.293-.584.41a2.072 2.072 0 01-.856.166zm1.038-2.334v-.022c0-.349-.096-.632-.288-.849-.187-.222-.44-.333-.758-.333s-.57.11-.757.333c-.182.217-.273.5-.273.849v.022h2.076z" />
    <path d="M2.647 5.42l-.041.042.04-.041zm-.038-.189l-.054-.02-.001.002.055.018zM4.488.33l.054.021-.054-.02zm1 0l-.055.02V.35l.055-.02zm1.871 4.902l.058-.01a.054.054 0 00-.003-.01l-.055.02zm.015.083h.059l-.001-.01-.058.01zm-.045.107l.04.04-.04-.04zm-.621.007l-.042.041a.042.042 0 00.004.004l.038-.045zm-.06-.09l.054-.02v-.002l-.055.021zM6.23 4.26l.055-.02a.058.058 0 00-.055-.038v.058zm-2.485 0v-.058a.058.058 0 00-.054.037l.054.021zM3.33 5.337l-.055-.02v.002l.055.018zm-.069.091l.038.045-.038-.045zM6.01 3.61v.058a.058.058 0 00.055-.079l-.054.02zM4.989.92L5.042.9a.058.058 0 00-.109 0l.055.02zM3.965 3.61l-.054-.02a.058.058 0 00.054.078V3.61zM2.753 5.408a.087.087 0 01-.065-.029l-.082.083c.04.04.09.062.147.062v-.116zm-.065-.029a.088.088 0 01-.028-.065h-.117c0 .057.022.107.063.148l.082-.083zm-.028-.065c0-.04.002-.06.004-.064l-.11-.037a.352.352 0 00-.01.101h.116zm.003-.062L4.543.35 4.432.31 2.555 5.21l.108.042zM4.543.349a.191.191 0 01.069-.097.204.204 0 01.118-.031V.104a.319.319 0 00-.184.052.307.307 0 00-.113.153l.11.04zM4.73.221h.515V.104H4.73v.117zm.515 0c.052 0 .09.012.119.031.028.02.052.05.07.097l.109-.04A.307.307 0 005.43.156a.319.319 0 00-.185-.052v.117zm.188.13l1.872 4.9.109-.04L5.542.308 5.433.35zm1.869 4.89l.015.084.115-.02-.015-.084-.115.02zm.014.073a.088.088 0 01-.028.065l.082.083c.04-.04.063-.09.063-.148h-.117zm-.028.065a.087.087 0 01-.065.029v.116a.203.203 0 00.147-.062l-.082-.083zm-.065.029h-.387v.116h.387v-.116zm-.387 0c-.047 0-.075-.011-.091-.025l-.075.09a.253.253 0 00.166.051v-.116zm-.087-.021a.179.179 0 01-.047-.068l-.11.037c.014.04.04.079.074.113l.083-.082zm-.048-.07L6.285 4.24l-.109.042.417 1.076.108-.042zm-.47-1.114H3.744v.117H6.23v-.117zm-2.54.037l-.417 1.076.109.042.417-1.076-.109-.042zM3.273 5.32a.16.16 0 01-.05.065l.075.088a.273.273 0 00.086-.116l-.11-.037zm-.05.064c-.017.015-.043.025-.084.025v.116a.24.24 0 00.159-.051l-.075-.09zm-.084.025h-.386v.116h.386v-.116zm2.926-1.819L5.042.9l-.109.042 1.023 2.69.11-.042zM4.933.9L3.911 3.59l.109.042L5.042.94 4.933.9zm-.968 2.77h2.046v-.117H3.965v.116zM8.14 6.851l-.041.042.04-.042zm0-5.273l.041.042-.041-.042zm.598 0l-.044.038a.114.114 0 00.006.007l.038-.045zm.053.455h-.058a.058.058 0 00.106.034l-.047-.034zm2.432-.083l-.045.036.045-.036zm.432 1.288l-.058.002v.003l.058-.005zm0 .515l-.058-.005v.003l.058.002zm-.432 1.288l.046.036-.046-.036zm-2.416-.068l.046-.036a.058.058 0 00-.104.036h.058zM8.76 6.852l-.041-.04a.028.028 0 00-.003.003l.044.037zm1.894-2.257l-.046-.035.046.035zm.273-.88l-.058-.005v.003l.058.003zM9.086 2.414l-.046-.035.046.035zm-.28.818l-.059-.003v.002l.059.001zm-.008.288l-.058-.002v.003l.058-.001zm.008.295l-.059.002.059-.002zm.288.78l.045-.036-.045.037zm-.826 2.253a.117.117 0 01-.088-.036l-.082.083c.046.046.104.07.17.07v-.117zm-.088-.036c-.017-.017-.028-.042-.028-.08h-.117c0 .063.02.12.063.163l.082-.083zm-.028-.08v-5.03h-.117v5.03h.117zm0-5.03c0-.038.01-.063.028-.08l-.082-.083a.222.222 0 00-.063.163h.117zm.028-.08a.117.117 0 01.088-.036v-.117a.233.233 0 00-.17.07l.082.083zm.088-.036h.348v-.117h-.348v.117zm.348 0c.035 0 .059.011.077.032l.089-.075a.21.21 0 00-.166-.074v.117zm.083.039a.093.093 0 01.033.077h.117a.21.21 0 00-.074-.166l-.076.089zm.033.077v.333h.117v-.333h-.117zm.106.367c.27-.371.665-.56 1.195-.56v-.116c-.561 0-.994.202-1.29.608l.095.068zm1.195-.56c.517 0 .895.163 1.144.479l.091-.072c-.276-.351-.692-.523-1.235-.523v.117zm1.144.48c.258.32.4.737.419 1.253l.117-.005c-.021-.534-.168-.977-.445-1.322l-.091.073zm.42 1.256c.004.053.007.136.007.252h.116a3.14 3.14 0 00-.008-.263l-.116.01zm.007.252c0 .116-.003.2-.008.253l.116.01a3.14 3.14 0 00.008-.263h-.116zm-.008.256c-.02.51-.16.927-.42 1.254l.092.072c.277-.35.424-.792.444-1.322l-.116-.004zm-.42 1.253c-.253.317-.63.479-1.143.479V5.6c.538 0 .954-.172 1.235-.522l-.091-.073zm-1.143.479c-.51 0-.9-.183-1.181-.546l-.092.071c.305.395.732.591 1.273.591v-.116zm-1.286-.51V6.73h.117V4.974h-.117zm0 1.757c0 .038-.01.063-.028.08l.082.083a.222.222 0 00.063-.163h-.117zm-.031.084a.094.094 0 01-.077.032v.117a.21.21 0 00.165-.074l-.088-.075zm-.077.032h-.371v.117h.37v-.117zm1.235-1.86c.37 0 .651-.116.827-.358l-.094-.068c-.147.202-.387.309-.733.309v.116zm.827-.357c.175-.234.269-.54.284-.912l-.116-.005c-.015.356-.104.637-.261.847l.093.07zm.284-.908c.005-.054.008-.13.008-.226h-.117c0 .096-.002.167-.007.214l.116.012zm.008-.226c0-.483-.09-.855-.277-1.108-.19-.257-.474-.382-.842-.382v.117c.339 0 .585.113.748.334.167.224.254.567.254 1.04h.117zm-1.12-1.49c-.362 0-.643.122-.833.372l.092.07c.164-.215.408-.325.742-.325v-.117zm-.833.372c-.18.237-.277.522-.293.85l.117.006c.015-.308.104-.57.268-.786l-.092-.07zm-.293.852l-.007.287.116.003.008-.287-.117-.003zm-.007.29l.007.296.117-.003-.008-.296-.116.003zm.007.296c.01.31.111.583.301.816l.09-.074a1.207 1.207 0 01-.274-.746l-.117.004zm.301.816c.195.238.474.354.826.354V4.87c-.325 0-.567-.106-.735-.312l-.09.074zm3.96.758l.025-.052-.025.052zm-.507-.409l-.048.033v.002l.048-.035zm.227-1.401l.036.046-.036-.046zm1.121-.425l.008.058-.008-.058zm1.129-.159l.008.058a.058.058 0 00.05-.058h-.058zm-1.432-.848l.032.049v-.001l-.032-.048zm-.318.325l-.051-.028a.06.06 0 00-.004.01l.055.018zm-.06.099l-.037-.046-.005.005.041.04zm-.539-.015l-.044.038a.052.052 0 00.006.006l.038-.044zm-.045-.121l-.058-.003v.003h.058zm.167-.41l-.048-.033.048.033zm.507-.401l.026.052-.026-.052zm2.06.227l-.042.04.043-.04zm.319 3.569l.041.04-.041-.04zm-.598 0l-.045.037a.052.052 0 00.007.007l.038-.044zm-.046-.455h.058a.058.058 0 00-.106-.033l.048.033zm-.485.41l-.027-.053.027.052zm.152-.72l-.042-.041.042.04zm.318-1.145h.058a.058.058 0 00-.067-.057l.009.057zm-.879.13l.008.057-.008-.058zm-.81.257l.03.049h.002l-.033-.05zm-.023.916l-.033.049h.001l.032-.049zm.431.67c-.239 0-.46-.05-.664-.146l-.05.105c.22.105.459.157.714.157v-.116zm-.664-.146a1.264 1.264 0 01-.486-.391l-.093.069c.132.18.31.322.529.427l.05-.105zm-.484-.39a.952.952 0 01-.172-.55h-.117c0 .224.065.43.193.616l.096-.066zm-.172-.55c0-.315.127-.57.386-.772l-.071-.092a1.05 1.05 0 00-.432.864h.117zm.387-.773c.267-.209.63-.347 1.093-.412l-.016-.115c-.476.066-.86.21-1.149.436l.072.091zm1.093-.412l1.13-.16-.017-.114-1.129.159.016.115zm1.18-.217v-.22h-.117v.22h.116zm0-.22c0-.268-.078-.48-.242-.623-.163-.142-.4-.208-.703-.208v.117c.288 0 .493.063.626.179.131.114.202.29.202.535h.116zm-.945-.83c-.231 0-.425.05-.579.153l.066.097a.895.895 0 01.513-.134v-.117zm-.577.153a.988.988 0 00-.338.346l.102.057a.874.874 0 01.299-.305l-.063-.098zm-.342.356a.152.152 0 01-.042.071l.073.091a.262.262 0 00.079-.125l-.11-.037zm-.047.076c-.005.005-.018.013-.05.013v.116a.18.18 0 00.133-.047l-.083-.082zm-.05.013h-.325v.116h.325v-.116zm-.325 0a.122.122 0 01-.084-.031l-.076.088c.046.04.1.06.16.06v-.117zm-.077-.025a.122.122 0 01-.032-.083h-.116c0 .06.02.113.06.159l.088-.076zm-.032-.081c.004-.106.053-.231.156-.378l-.095-.067c-.11.156-.172.303-.177.44l.116.005zm.156-.378c.104-.146.264-.275.486-.383l-.052-.104a1.369 1.369 0 00-.529.42l.095.067zm.487-.383c.216-.111.487-.168.814-.168v-.117c-.34 0-.63.06-.867.181l.053.104zm.814-.168c.559 0 .945.132 1.177.383l.086-.08c-.264-.284-.69-.42-1.263-.42v.117zm1.177.383c.237.25.356.564.356.945h.116c0-.407-.128-.75-.387-1.025l-.085.08zm.356.945v2.455h.116V2.837h-.116zm0 2.455a.117.117 0 01-.036.087l.082.083a.234.234 0 00.07-.17h-.116zm-.036.087c-.018.018-.042.029-.08.029v.116c.063 0 .12-.02.162-.062l-.082-.083zm-.08.029h-.349v.116h.349v-.116zm-.349 0a.134.134 0 01-.09-.032L15 5.465a.25.25 0 00.166.06v-.117zm-.084-.025a.133.133 0 01-.032-.091h-.116c0 .063.02.12.06.166l.088-.075zm-.032-.091v-.326h-.116v.326h.116zm-.106-.359a1.253 1.253 0 01-.464.39l.054.104c.219-.115.389-.257.506-.428l-.096-.066zm-.464.39c-.201.106-.461.16-.784.16V5.6c.335 0 .615-.057.838-.173l-.054-.103zm-.624-.291c.342 0 .625-.111.845-.336l-.084-.081c-.195.2-.447.3-.761.3v.117zm.845-.336c.225-.231.334-.558.334-.972h-.116c0 .394-.104.688-.302.89l.083.082zm.334-.972V3.51h-.116v.212h.116zm-.067-.27l-.878.129.017.115.878-.129-.017-.115zm-.878.128c-.362.052-.643.14-.835.267l.065.097c.171-.114.431-.198.786-.248l-.016-.116zm-.834.267c-.195.124-.3.29-.3.496h.117c0-.158.077-.29.246-.398l-.063-.098zm-.3.496a.59.59 0 00.276.518l.065-.097a.475.475 0 01-.224-.421h-.117zm.277.519c.178.112.386.168.623.168v-.117c-.218 0-.404-.05-.56-.15l-.063.099zm3.863.223l-.041.041.041-.04zM16.612 3.8l-.058.002.058-.002zm-.007-.303l-.058-.001v.003l.058-.002zm.007-.303l-.058-.002.058.002zm.485-1.28l.042.041-.042-.041zm2.197-.265l-.026.051.026-.051zm.561.484l-.05.032h.002l.048-.032zm.205.576l-.058.003v.003l.058-.006zm-.046.122l.038.044-.038-.044zm-.606.015l.037-.046-.037.046zm-.076-.122l-.055.02.002.003.053-.023zm-.363-.515l-.033.048.001.001.032-.049zm-1.341.137l-.043-.04.043.04zm-.296.886l-.058-.002.058.002zm-.007.273l-.058-.002v.004l.058-.002zm.007.257l-.058.002.058-.002zm.296.894l-.043.04.043-.04zm1.34.137L19 4.84l.001-.001-.032-.048zm.364-.523l-.053-.022-.002.003.055.019zm.076-.121l.037.045.004-.004-.04-.041zm.606.015l-.041.041.003.003.038-.044zm.046.121l-.058-.006v.003l.058.003zm-.205.576l-.049-.032.05.032zm-.56.485l-.027-.052.026.052zm-.925.139c-.529 0-.937-.149-1.232-.438l-.081.083c.321.316.761.471 1.313.471v-.116zm-1.231-.438c-.29-.295-.448-.709-.468-1.249l-.117.004c.02.561.186 1.006.502 1.327l.083-.082zm-.468-1.248l-.008-.303-.116.003.007.303.117-.003zm-.008-.3l.008-.303-.117-.003-.007.303.116.003zm.008-.303c.02-.54.178-.951.468-1.241l-.083-.082c-.316.316-.481.758-.502 1.319l.117.004zm.468-1.241c.294-.295.702-.445 1.231-.445v-.117c-.552 0-.992.158-1.314.48l.083.082zm1.231-.445c.357 0 .655.064.898.19l.053-.103c-.263-.137-.58-.204-.95-.204v.117zm.898.19c.245.128.423.283.538.465l.098-.063a1.468 1.468 0 00-.582-.505l-.054.103zm.538.466c.122.184.186.367.195.546l.117-.006a1.215 1.215 0 00-.214-.605l-.098.065zm.196.55c.003.033-.006.054-.026.07l.076.089a.19.19 0 00.066-.171l-.116.011zm-.026.07a.133.133 0 01-.09.032v.116a.25.25 0 00.166-.06l-.076-.088zm-.09.032h-.364v.116h.364v-.116zm-.364 0c-.044 0-.067-.01-.077-.018l-.073.091c.04.032.093.043.15.043v-.116zm-.077-.018a.264.264 0 01-.059-.098l-.107.045a.364.364 0 00.093.144l.073-.09zm-.058-.095c-.088-.25-.216-.434-.387-.545l-.063.098c.142.092.257.251.34.486l.11-.039zm-.386-.544a1.089 1.089 0 00-.623-.17v.117c.229 0 .413.051.558.15l.065-.097zm-.623-.17c-.327 0-.593.104-.793.315l.085.08c.174-.184.408-.278.708-.278v-.116zm-.793.315c-.195.206-.296.518-.311.924l.116.004c.015-.392.112-.67.28-.848l-.085-.08zm-.311.924l-.007.273.116.003.008-.272-.117-.004zm-.007.276l.007.258.116-.003-.007-.258-.116.004zm.007.259c.015.41.116.725.311.931l.085-.08c-.168-.177-.265-.459-.28-.856l-.116.005zm.311.931c.195.206.462.306.793.306v-.116c-.306 0-.54-.092-.708-.27l-.085.08zm.793.306c.245 0 .454-.052.622-.16l-.063-.098c-.145.093-.33.142-.56.142v.116zM19 4.84c.17-.115.298-.302.386-.552l-.11-.038c-.083.234-.198.397-.34.494L19 4.84zm.385-.549a.264.264 0 01.059-.098l-.073-.09a.363.363 0 00-.093.144l.107.044zm.063-.102c.01-.01.031-.02.073-.02v-.117a.212.212 0 00-.155.055l.082.082zm.073-.02h.364v-.117h-.364v.116zm.364 0c.035 0 .063.01.087.035l.082-.082a.234.234 0 00-.17-.07v.116zm.09.038c.02.017.03.038.026.071l.116.012a.19.19 0 00-.066-.171l-.076.088zm.026.074a1.13 1.13 0 01-.196.547l.098.063c.13-.203.203-.405.214-.604l-.116-.006zm-.196.548a1.351 1.351 0 01-.538.464l.053.103c.26-.134.455-.302.583-.505l-.098-.062zm-.539.464c-.237.126-.535.19-.897.19V5.6c.376 0 .694-.067.952-.204l-.055-.103zm1.745.128l-.044.037a.052.052 0 00.006.007l.038-.044zm0-5.281l.042.041-.041-.041zm.63 0l-.045.038a.115.115 0 00.006.006l.038-.044zm.052 1.886h-.058a.058.058 0 00.104.036l-.046-.036zm.515-.424l.026.053.001-.001-.027-.052zm1.864.318l-.044.038v.001l.044-.039zm.356 3.5l-.041-.04.041.04zm-.628 0l-.045.038.006.007.038-.044zm-.311-3.06l-.044.038.044-.038zm-1.515.007l-.043-.04.043.04zm-.334 3.054l-.041-.042.041.042zm-.5-.013a.132.132 0 01-.09-.032l-.076.089a.25.25 0 00.166.06v-.117zm-.084-.025a.133.133 0 01-.032-.091h-.116c0 .063.02.12.06.166l.088-.075zm-.032-.091V.262h-.116v5.03h.116zm0-5.03c0-.039.011-.063.029-.08L20.97.098a.222.222 0 00-.062.162h.116zm.029-.08a.117.117 0 01.087-.037V.03a.233.233 0 00-.17.07l.083.082zm.087-.037h.38V.03h-.38v.116zm.38 0c.034 0 .058.012.076.033l.088-.076A.21.21 0 0021.52.03v.116zm.082.04a.093.093 0 01.033.076h.117a.21.21 0 00-.074-.165l-.076.088zm.033.076v1.765h.117V.261h-.117zm.104 1.801a1.41 1.41 0 01.495-.407l-.051-.105c-.206.1-.384.247-.536.44l.092.072zm.496-.408c.187-.096.422-.145.708-.145v-.117c-.299 0-.554.052-.761.158l.053.104zm.708-.145c.472 0 .83.151 1.085.45l.088-.076c-.28-.329-.674-.49-1.173-.49v.116zm1.086.45c.26.295.394.692.394 1.196h.117c0-.525-.14-.952-.424-1.273l-.087.077zm.394 1.196v2.137h.117V3.155h-.117zm0 2.137a.117.117 0 01-.036.087l.082.083a.234.234 0 00.07-.17h-.116zm-.036.087c-.017.018-.042.029-.08.029v.116c.063 0 .12-.02.163-.062l-.083-.083zm-.08.029h-.379v.116h.38v-.116zm-.379 0a.133.133 0 01-.09-.032l-.076.089a.25.25 0 00.166.06v-.117zm-.084-.025a.133.133 0 01-.032-.091h-.116c0 .063.02.12.06.166l.088-.075zm-.032-.091V3.193h-.116v2.099h.116zm0-2.099c0-.368-.09-.661-.28-.872l-.087.078c.165.183.25.445.25.794h.117zm-.28-.871c-.185-.213-.453-.316-.793-.316v.117c.316 0 .548.094.706.275l.088-.076zm-.793-.316c-.332 0-.603.106-.808.321l.085.08c.18-.188.418-.284.723-.284v-.117zm-.808.321c-.2.211-.296.502-.296.866h.117c0-.343.09-.602.264-.786l-.085-.08zm-.296.866v2.099h.117V3.193h-.117zm0 2.099a.117.117 0 01-.036.087l.082.083a.234.234 0 00.07-.17h-.116zm-.036.087c-.018.018-.042.029-.08.029v.116c.063 0 .12-.02.163-.062l-.083-.083zm-.08.029h-.379v.116h.38v-.116zm4.382-.344l-.042.04v.001l.042-.04zm-.508-1.318l-.058.002v.001l.058-.003zm-.007-.257l-.059-.002v.003l.059-.001zm.007-.25l-.058-.004v.002l.058.002zm.515-1.303l-.041-.041.041.04zm2.539.053l-.045.038.045-.038zm.409 1.697l.04.04-.04-.04zm-2.743.045v-.058a.058.058 0 00-.058.058h.058zm0 .068h-.058v.003l.058-.003zm.296.841l-.046.036v.001l.046-.037zm1.302.205l.031.05v-.001l-.03-.05zm.334-.296l-.047-.035v.002l.047.033zm.098-.106l.027.052.003-.002-.03-.05zm.614.015l-.037.045.037-.045zm-.166.508l-.044-.04v.002l.044.038zm-.584.41l.024.052-.024-.053zm.182-2.168v.059a.058.058 0 00.058-.059h-.058zm-.288-.87l-.045.037h.001l.044-.038zm-1.515 0l-.045-.038.045.037zm-.273.87h-.058c0 .032.026.059.058.059v-.059zm1.038 2.276c-.507 0-.908-.155-1.209-.46l-.083.081c.326.331.759.495 1.292.495v-.116zm-1.208-.46c-.296-.31-.462-.736-.492-1.281l-.116.006c.031.566.204 1.02.524 1.356l.084-.08zm-.491-1.28l-.008-.257-.117.003.008.258.117-.003zm-.008-.254l.008-.25-.117-.003-.008.25.117.003zm.007-.248c.035-.535.203-.955.5-1.266l-.085-.08c-.32.335-.495.783-.53 1.339l.115.007zm.5-1.266c.3-.31.696-.467 1.192-.467v-.117c-.523 0-.95.167-1.276.503l.083.081zm1.192-.467c.548 0 .964.174 1.26.518l.088-.076c-.322-.374-.773-.559-1.348-.559v.117zm1.26.518c.296.34.447.805.447 1.401h.117c0-.616-.157-1.11-.477-1.478l-.087.077zm.447 1.401v.129h.117v-.129h-.117zm0 .129a.117.117 0 01-.036.087l.083.083a.234.234 0 00.07-.17h-.117zm-.036.087c-.017.018-.042.029-.08.029v.116c.063 0 .12-.02.163-.062l-.083-.083zm-.08.029h-2.621v.116h2.621v-.116zm-2.68.058V3.8h.117v-.068h-.116zm.001.07c.016.34.118.632.308.875l.091-.072a1.377 1.377 0 01-.283-.807l-.116.005zm.309.876c.198.241.46.362.78.362v-.116a.847.847 0 01-.69-.32l-.09.074zm.78.362c.239 0 .44-.046.598-.145l-.061-.099c-.135.084-.312.128-.538.128v.116zm.599-.145c.155-.098.273-.202.35-.311l-.096-.067c-.064.092-.169.186-.317.28l.063.098zm.349-.31a.834.834 0 01.072-.084l.006-.005-.053-.104a.219.219 0 00-.056.047 1 1 0 00-.063.076l.094.07zm.081-.09c.011-.007.04-.015.1-.015v-.117a.317.317 0 00-.16.031l.06.1zm.1-.015h.37v-.117h-.37v.117zm.37 0c.035 0 .059.009.077.024l.075-.09a.23.23 0 00-.151-.05v.116zm.077.024c.015.013.024.03.024.061h.117c0-.06-.02-.112-.066-.15l-.075.09zm.024.061c0 .089-.057.209-.197.363l.087.078c.143-.159.227-.307.227-.44h-.117zm-.198.364c-.129.148-.316.28-.563.394l.048.106c.258-.119.46-.26.604-.424l-.089-.076zm-.563.394a2.012 2.012 0 01-.832.162V5.6c.33 0 .624-.057.88-.172l-.048-.106zm.264-2.114v-.022h-.116v.022h.116zm0-.022c0-.36-.1-.658-.302-.888l-.088.078c.181.204.274.472.274.81h.116zm-.301-.886c-.2-.237-.47-.355-.803-.355v.117c.303 0 .539.105.713.313l.09-.075zm-.803-.355c-.333 0-.603.118-.802.355l.09.075c.174-.208.41-.313.712-.313v-.117zm-.802.355c-.192.23-.286.527-.286.886h.116c0-.339.088-.607.26-.812l-.09-.074zm-.286.886v.022h.116v-.022h-.116zm.058.08h2.076V3.15h-2.076v.117zM1.178 23.26a.37.37 0 01-.27-.116.37.37 0 01-.116-.27c0-.103.007-.173.02-.212L5.585 10.21c.103-.282.308-.423.616-.423H7.51c.308 0 .513.141.616.423l4.754 12.453.038.212a.37.37 0 01-.115.27.37.37 0 01-.27.115h-.981a.499.499 0 01-.327-.096.608.608 0 01-.154-.231l-1.059-2.733H3.7L2.64 22.932a.546.546 0 01-.173.231.467.467 0 01-.308.096h-.981zm8.276-4.716L6.856 11.71l-2.599 6.833h5.197zm5.561-7.16a.486.486 0 01-.327-.116.485.485 0 01-.116-.327V9.9c0-.128.039-.237.116-.327a.446.446 0 01.327-.135h1.213c.128 0 .237.045.327.135.09.09.134.199.134.327v1.04a.445.445 0 01-.134.327.485.485 0 01-.327.115h-1.213zm.154 11.875a.487.487 0 01-.327-.115.485.485 0 01-.116-.328v-9.123c0-.128.039-.23.116-.308a.446.446 0 01.327-.135h.924c.128 0 .23.045.308.135.09.077.135.18.135.308v9.123a.446.446 0 01-.135.328.416.416 0 01-.308.115h-.924zm4.662 0a.486.486 0 01-.328-.115.486.486 0 01-.115-.328v-9.104c0-.128.038-.237.115-.327a.446.446 0 01.328-.135h.885c.128 0 .237.045.327.135.09.09.135.199.135.327v.847c.526-.872 1.424-1.309 2.694-1.309h.751c.128 0 .231.045.308.135.09.077.135.18.135.308v.79a.385.385 0 01-.135.307.416.416 0 01-.308.116h-1.155c-.693 0-1.238.205-1.636.616-.398.397-.597.943-.597 1.636v5.658a.445.445 0 01-.134.328.487.487 0 01-.328.115h-.942zm8.312 0a.486.486 0 01-.327-.115.485.485 0 01-.116-.328V14.79h-1.636a.485.485 0 01-.327-.115.485.485 0 01-.115-.328v-.654c0-.128.038-.23.115-.308a.445.445 0 01.327-.135H27.7v-.962c0-2.181 1.104-3.272 3.31-3.272h1.079c.128 0 .23.045.308.135.09.077.135.18.135.308v.654a.445.445 0 01-.135.327.416.416 0 01-.308.116h-1.04c-.577 0-.981.154-1.212.462-.231.295-.347.75-.347 1.366v.866h4.12V9.46c0-.129.038-.231.115-.308a.446.446 0 01.327-.135h.885c.129 0 .232.045.308.135.09.077.135.18.135.308v13.357a.445.445 0 01-.135.328.416.416 0 01-.308.115h-.885a.487.487 0 01-.327-.115.487.487 0 01-.116-.328V14.79H29.49v8.026a.446.446 0 01-.134.328.417.417 0 01-.308.115h-.905zm14.197.193c-1.411 0-2.508-.398-3.291-1.194-.77-.795-1.18-1.854-1.232-3.176l-.02-.827.02-.828c.051-1.309.468-2.36 1.251-3.156.783-.809 1.874-1.213 3.272-1.213 1.399 0 2.49.404 3.272 1.213.783.795 1.2 1.847 1.252 3.156.012.141.019.417.019.828 0 .41-.007.686-.02.827-.051 1.322-.468 2.38-1.25 3.176-.77.796-1.861 1.194-3.273 1.194zm0-1.483c.809 0 1.444-.256 1.906-.77.475-.513.731-1.25.77-2.213.013-.128.019-.372.019-.731 0-.36-.006-.603-.02-.732-.038-.962-.294-1.7-.77-2.213-.461-.513-1.096-.77-1.905-.77-.808 0-1.45.257-1.924.77-.475.513-.725 1.251-.751 2.213l-.02.732.02.731c.026.962.276 1.7.75 2.214.475.513 1.117.77 1.925.77zm9.346 1.29c-.167 0-.295-.038-.385-.115a.9.9 0 01-.231-.366l-2.733-8.931-.039-.192c0-.116.039-.212.116-.29a.436.436 0 01.288-.115h.847c.129 0 .231.039.308.116.09.064.148.135.174.212l2.136 7.256 2.29-7.18a.518.518 0 01.174-.269.495.495 0 01.365-.135h.655c.154 0 .276.045.366.135.09.077.147.167.173.27l2.29 7.179 2.137-7.256a.445.445 0 01.134-.212.486.486 0 01.328-.116h.866a.37.37 0 01.27.116.392.392 0 01.115.289l-.039.192-2.714 8.93a.9.9 0 01-.23.367c-.09.077-.225.115-.405.115h-.75c-.347 0-.559-.16-.636-.481l-2.232-6.89-2.233 6.89c-.103.32-.32.481-.654.481h-.751z" />
  </svg>
);

export default ApacheAirflowLogo;
